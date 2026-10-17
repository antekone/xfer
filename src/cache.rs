//! Per-connection typed directory cache, shared by listing and completion.
use crate::{
    backend::Connection,
    core::{SettingValue, Settings},
    listing::{Content, ListingSink},
};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

const MAX_DIRECTORIES: usize = 32;
const MAX_ENTRIES: usize = 2048;
const MAX_DIRECTORY_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct CacheOptions {
    pub ttl: Duration,
    pub max_bytes: usize,
}
impl CacheOptions {
    pub fn register(settings: &mut Settings) {
        settings.register(
            "cache:ttl",
            SettingValue::Int(30),
            "Directory cache TTL in seconds (0 disables caching; reconnect to apply)",
        );
        settings.register(
            "cache:max-bytes",
            SettingValue::Int(4 * 1024 * 1024),
            "Per-connection directory cache budget in bytes (reconnect to apply)",
        );
    }
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let integer = |name: &str| match settings.values.get(name).map(|setting| &setting.value) {
            Some(SettingValue::Int(value)) if *value >= 0 => Ok(*value as u64),
            _ => anyhow::bail!("{name} must be a non-negative integer"),
        };
        Ok(Self {
            ttl: Duration::from_secs(integer("cache:ttl")?),
            max_bytes: usize::try_from(integer("cache:max-bytes")?)?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PathCompletion {
    pub candidates: Vec<String>,
    pub truncated: bool,
}

#[derive(Clone)]
struct Snapshot {
    entries: Arc<[Content]>,
    truncated: bool,
    bytes: usize,
}
struct CacheEntry {
    path: String,
    inserted: Instant,
    snapshot: Snapshot,
}
struct DirectoryCache {
    options: CacheOptions,
    entries: VecDeque<CacheEntry>,
    bytes: usize,
}
impl DirectoryCache {
    fn purge(&mut self) {
        let now = Instant::now();
        self.entries
            .retain(|entry| now.duration_since(entry.inserted) < self.options.ttl);
        self.bytes = self.entries.iter().map(|entry| entry.snapshot.bytes).sum();
    }
    fn get(&mut self, path: &str) -> Option<Snapshot> {
        self.purge();
        let index = self.entries.iter().position(|entry| entry.path == path)?;
        let entry = self.entries.remove(index)?;
        let snapshot = entry.snapshot.clone();
        self.entries.push_back(entry);
        Some(snapshot)
    }
    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }
    fn invalidate(&mut self, path: &str) {
        if let Some(index) = self.entries.iter().position(|entry| entry.path == path) {
            self.bytes -= self.entries.remove(index).unwrap().snapshot.bytes;
        }
    }
    fn insert(&mut self, path: String, mut snapshot: Snapshot) {
        self.purge();
        self.invalidate(&path);
        snapshot.bytes += path.len() + 128;
        if self.options.ttl.is_zero() || snapshot.bytes > self.options.max_bytes {
            return;
        }
        while self.entries.len() >= MAX_DIRECTORIES
            || self.bytes + snapshot.bytes > self.options.max_bytes
        {
            if let Some(old) = self.entries.pop_front() {
                self.bytes -= old.snapshot.bytes;
            } else {
                break;
            }
        }
        self.bytes += snapshot.bytes;
        self.entries.push_back(CacheEntry {
            path,
            inserted: Instant::now(),
            snapshot,
        });
    }
}

#[derive(Default)]
struct Records {
    entries: Vec<Content>,
    bytes: usize,
    truncated: bool,
}
impl Records {
    fn push(&mut self, entry: Content) {
        if self.truncated {
            return;
        }
        let name_bytes = match &entry {
            Content::Directory(info) => info.name.len(),
            Content::Bucket(info) => info.name.len(),
            Content::File(info) => info.name.len(),
            Content::Comment(info) => info.text.len(),
        };
        let bytes = name_bytes + 128;
        if self.entries.len() >= MAX_ENTRIES || self.bytes + bytes > MAX_DIRECTORY_BYTES {
            self.truncated = true;
            return;
        }
        self.bytes += bytes;
        self.entries.push(entry);
    }
    fn snapshot(self) -> Snapshot {
        Snapshot {
            entries: self.entries.into(),
            truncated: self.truncated,
            bytes: self.bytes,
        }
    }
}
#[derive(Debug)]
struct CompletionLimit;
impl std::fmt::Display for CompletionLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("completion listing limit reached")
    }
}
impl std::error::Error for CompletionLimit {}
impl ListingSink for Records {
    fn emit(&mut self, entry: Content) -> Result<()> {
        self.push(entry);
        if self.truncated {
            return Err(CompletionLimit.into());
        }
        Ok(())
    }
}
struct Tee<'a> {
    output: &'a mut dyn ListingSink,
    records: Records,
}
impl ListingSink for Tee<'_> {
    fn emit(&mut self, entry: Content) -> Result<()> {
        self.output.emit(entry.clone())?;
        self.records.push(entry);
        Ok(())
    }
}

pub struct CachedConnection {
    inner: Box<dyn Connection>,
    cache: Mutex<DirectoryCache>,
}
impl CachedConnection {
    pub fn new(inner: Box<dyn Connection>, options: CacheOptions) -> Self {
        Self {
            inner,
            cache: Mutex::new(DirectoryCache {
                options,
                entries: VecDeque::new(),
                bytes: 0,
            }),
        }
    }
}

impl CachedConnection {
    async fn complete(&self, input: &str, include_files: bool) -> Result<PathCompletion> {
        let (parent, prefix) = match input.rfind('/') {
            Some(index) => (&input[..=index], &input[index + 1..]),
            None => ("", input),
        };
        let canonical = self.inner.resolve_directory(Some(parent))?;
        let cached = self.cache.lock().unwrap().get(&canonical);
        let snapshot = match cached {
            Some(snapshot) => snapshot,
            None => {
                let mut records = Records::default();
                if let Err(error) = self.inner.list(&canonical, &mut records).await {
                    if !error.is::<CompletionLimit>() {
                        return Err(error);
                    }
                }
                let snapshot = records.snapshot();
                self.cache
                    .lock()
                    .unwrap()
                    .insert(canonical, snapshot.clone());
                snapshot
            }
        };
        let mut names = BTreeSet::new();
        if "../".starts_with(prefix) {
            names.insert(format!("{parent}../"));
        }
        for entry in snapshot.entries.iter() {
            let (name, directory) = match entry {
                Content::Directory(info) => (&info.name, true),
                Content::Bucket(info) => (&info.name, true),
                Content::File(info) if include_files => (&info.name, false),
                _ => continue,
            };
            if name.starts_with(prefix) {
                let slash = if directory && !name.ends_with('/') {
                    "/"
                } else {
                    ""
                };
                names.insert(format!("{parent}{name}{slash}"));
            }
        }
        Ok(PathCompletion {
            candidates: names.into_iter().collect(),
            truncated: snapshot.truncated,
        })
    }
}

#[async_trait]
impl Connection for CachedConnection {
    fn prompt_user(&self) -> String {
        self.inner.prompt_user()
    }
    fn prompt_host(&self) -> String {
        self.inner.prompt_host()
    }

    fn protocol(&self) -> &'static str {
        self.inner.protocol()
    }
    fn pwd(&self) -> Result<String> {
        self.inner.pwd()
    }
    fn resolve_directory(&self, path: Option<&str>) -> Result<String> {
        self.inner.resolve_directory(path)
    }
    async fn stat_file(&self, path: &str) -> Result<crate::transfer::RemoteFile> {
        self.inner.stat_file(path).await
    }
    async fn upload(&self, local: &std::path::Path, path: &str, overwrite: bool) -> Result<()> {
        self.cache.lock().unwrap().clear();
        let result = self.inner.upload(local, path, overwrite).await;
        self.cache.lock().unwrap().clear();
        result
    }
    async fn download(
        &self,
        path: &str,
        offset: u64,
        expected: &crate::transfer::RemoteFile,
        sink: &mut dyn crate::transfer::DownloadSink,
    ) -> Result<()> {
        self.inner.download(path, offset, expected, sink).await
    }
    fn resolve_listing_path(&self, path: &str) -> Result<String> {
        self.inner.resolve_listing_path(path)
    }
    async fn change_directory(&mut self, path: &str) -> Result<()> {
        self.inner.change_directory(path).await
    }
    async fn create_directory(&self, path: &str, parents: bool) -> Result<()> {
        // -p can change multiple ancestors, including through symlink aliases.
        // Invalidate before awaiting so partial failure/cancellation cannot leave
        // old completions behind. The daemon serializes connection operations.
        self.cache.lock().unwrap().clear();
        let result = self.inner.create_directory(path, parents).await;
        self.cache.lock().unwrap().clear();
        result
    }
    async fn disconnect(self: Box<Self>) -> Result<()> {
        self.inner.disconnect().await
    }
    async fn list(&self, path: &str, output: &mut dyn ListingSink) -> Result<u64> {
        let canonical = self.inner.resolve_listing_path(path)?;
        // A single-file result is not a directory snapshot.
        if !canonical.ends_with('/') {
            return self.inner.list(&canonical, output).await;
        }
        // Explicit ls always refreshes. A failed/partial refresh cannot leave a
        // supposedly fresh cached snapshot behind.
        self.cache.lock().unwrap().invalidate(&canonical);
        let mut tee = Tee {
            output,
            records: Records::default(),
        };
        let count = self.inner.list(&canonical, &mut tee).await?;
        self.cache
            .lock()
            .unwrap()
            .insert(canonical, tee.records.snapshot());
        Ok(count)
    }
    async fn complete_directories(&self, input: &str) -> Result<PathCompletion> {
        self.complete(input, false).await
    }
    async fn complete_paths(&self, input: &str) -> Result<PathCompletion> {
        self.complete(input, true).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listing::{BucketInfo, CommentInfo, DirectoryInfo, FileInfo};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn directory(name: &str) -> Content {
        Content::Directory(DirectoryInfo {
            unix: None,
            size: None,
            name: name.into(),
            modified: None,
        })
    }
    fn options() -> CacheOptions {
        CacheOptions {
            ttl: Duration::from_secs(30),
            max_bytes: 4 * 1024 * 1024,
        }
    }
    struct Fake {
        calls: Arc<AtomicUsize>,
        contents: Arc<Mutex<Vec<Content>>>,
        fail: Arc<AtomicBool>,
    }
    #[async_trait]
    impl Connection for Fake {
        fn protocol(&self) -> &'static str {
            "test"
        }
        fn pwd(&self) -> Result<String> {
            Ok("/".into())
        }
        fn resolve_directory(&self, path: Option<&str>) -> Result<String> {
            let path = path.unwrap_or("").trim_matches('/');
            Ok(if path.is_empty() {
                "/".into()
            } else {
                format!("/{path}/")
            })
        }
        async fn create_directory(&self, path: &str, _parents: bool) -> Result<()> {
            self.contents
                .lock()
                .unwrap()
                .push(directory(&format!("{}/", path.trim_matches('/'))));
            if self.fail.load(Ordering::SeqCst) {
                return Err(crate::error::NonRetryableError("partial mkdir failure".into()).into());
            }
            Ok(())
        }
        async fn list(&self, _path: &str, output: &mut dyn ListingSink) -> Result<u64> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                anyhow::bail!("offline");
            }
            let entries = self.contents.lock().unwrap().clone();
            for entry in &entries {
                output.emit(entry.clone())?;
            }
            Ok(entries.len() as u64)
        }
    }
    #[derive(Default)]
    struct Collect(Vec<Content>);
    impl ListingSink for Collect {
        fn emit(&mut self, entry: Content) -> Result<()> {
            self.0.push(entry);
            Ok(())
        }
    }
    fn connection() -> (
        CachedConnection,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<Content>>>,
        Arc<AtomicBool>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let contents = Arc::new(Mutex::new(vec![
            directory("photos/"),
            Content::Bucket(BucketInfo {
                name: "archive".into(),
                created: None,
            }),
            Content::File(FileInfo {
                unix: None,
                name: "photos.txt".into(),
                size: Some(1),
                modified: None,
            }),
            Content::Comment(CommentInfo { text: "hi".into() }),
        ]));
        let fail = Arc::new(AtomicBool::new(false));
        (
            CachedConnection::new(
                Box::new(Fake {
                    calls: calls.clone(),
                    contents: contents.clone(),
                    fail: fail.clone(),
                }),
                options(),
            ),
            calls,
            contents,
            fail,
        )
    }

    #[tokio::test]
    async fn completion_uses_cache_and_only_suggests_directories_buckets_and_parent() {
        let (connection, calls, _, _) = connection();
        assert_eq!(
            connection
                .complete_directories("")
                .await
                .unwrap()
                .candidates,
            vec!["../", "archive/", "photos/"]
        );
        assert_eq!(
            connection
                .complete_directories("pho")
                .await
                .unwrap()
                .candidates,
            vec!["photos/"]
        );
        assert_eq!(
            connection
                .complete_directories("..")
                .await
                .unwrap()
                .candidates,
            vec!["../"]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            connection
                .complete_directories("photos/ar")
                .await
                .unwrap()
                .candidates,
            vec!["photos/archive/"]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ls_completion_includes_files_and_shares_cache_with_cd() {
        let (connection, calls, _, _) = connection();
        assert_eq!(
            connection
                .complete_directories("")
                .await
                .unwrap()
                .candidates,
            vec!["../", "archive/", "photos/"]
        );
        assert_eq!(
            connection.complete_paths("").await.unwrap().candidates,
            vec!["../", "archive/", "photos.txt", "photos/"]
        );
        assert_eq!(
            connection.complete_paths("pho").await.unwrap().candidates,
            vec!["photos.txt", "photos/"]
        );
        assert_eq!(
            connection
                .complete_paths("archive/pho")
                .await
                .unwrap()
                .candidates,
            vec!["archive/photos.txt", "archive/photos/"]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2); // root + archive, not one request per completion kind
    }

    #[tokio::test]
    async fn explicit_ls_always_refreshes_and_populates_completion_cache() {
        let (connection, calls, contents, _) = connection();
        let mut output = Collect::default();
        assert_eq!(connection.list("", &mut output).await.unwrap(), 4);
        assert_eq!(output.0.len(), 4);
        connection.complete_directories("").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        *contents.lock().unwrap() = vec![directory("new/")];
        connection.list("", &mut output).await.unwrap();
        assert_eq!(
            connection
                .complete_directories("")
                .await
                .unwrap()
                .candidates,
            vec!["../", "new/"]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn mkdir_forwards_and_invalidates_completion_cache_even_after_partial_failure() {
        let (connection, calls, _, fail) = connection();
        assert!(connection
            .complete_directories("new")
            .await
            .unwrap()
            .candidates
            .is_empty());
        connection.create_directory("/new/", false).await.unwrap();
        assert_eq!(
            connection
                .complete_directories("new")
                .await
                .unwrap()
                .candidates,
            vec!["new/"]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        fail.store(true, Ordering::SeqCst);
        assert!(connection
            .create_directory("/partial/", true)
            .await
            .is_err());
        fail.store(false, Ordering::SeqCst);
        assert_eq!(
            connection
                .complete_directories("part")
                .await
                .unwrap()
                .candidates,
            vec!["partial/"]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_expiry_fetches_again_without_extending_ttl_on_hits() {
        let (connection, calls, _, _) = connection();
        connection.complete_directories("").await.unwrap();
        tokio::time::advance(Duration::from_secs(20)).await;
        connection.complete_directories("").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_secs(11)).await;
        connection.complete_directories("").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_refresh_does_not_serve_old_cached_results() {
        let (connection, calls, _, fail) = connection();
        connection.complete_directories("").await.unwrap();
        fail.store(true, Ordering::SeqCst);
        assert!(connection.list("", &mut Collect::default()).await.is_err());
        assert!(connection.complete_directories("").await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn completion_is_bounded_but_manual_ls_still_streams_all_entries() {
        let (connection, calls, contents, _) = connection();
        *contents.lock().unwrap() = (0..MAX_ENTRIES + 10)
            .map(|i| directory(&format!("d{i:04}/")))
            .collect();
        let result = connection.complete_directories("").await.unwrap();
        assert!(result.truncated);
        assert_eq!(result.candidates.len(), MAX_ENTRIES + 1); // includes ../
        connection.complete_directories("").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let mut all = Collect::default();
        connection.list("", &mut all).await.unwrap();
        assert_eq!(all.0.len(), MAX_ENTRIES + 10);
    }

    #[test]
    fn cache_evicts_lru_entries_and_obeys_byte_budget() {
        let snapshot = Records {
            entries: vec![directory("a/")],
            bytes: 130,
            truncated: false,
        }
        .snapshot();
        let mut cache = DirectoryCache {
            options: options(),
            entries: VecDeque::new(),
            bytes: 0,
        };
        for index in 0..=MAX_DIRECTORIES {
            cache.insert(format!("/{index}/"), snapshot.clone());
        }
        assert_eq!(cache.entries.len(), MAX_DIRECTORIES);
        assert!(cache.get("/0/").is_none());
        assert!(cache.get("/1/").is_some());
        cache.insert("/another/".into(), snapshot.clone());
        assert!(cache.get("/2/").is_none());
        assert!(cache.get("/1/").is_some());
        cache.options.max_bytes = 600;
        cache.insert("/small/".into(), snapshot.clone());
        assert!(cache.bytes <= 600);
        assert!(cache.entries.len() <= 2);
        cache.options.ttl = Duration::ZERO;
        cache.insert("/disabled/".into(), snapshot);
        assert!(cache.get("/disabled/").is_none());
    }
}

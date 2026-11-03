/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Private disk-backed directory snapshots: constant-memory collection, indexed
//! random-access pages, and automatic cleanup on cancellation/close/acknowledge.
use crate::{
    ipc::{self, pb},
    listing::{Content, ListingSink},
};
use anyhow::{bail, Result};
use prost::Message;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

mod sort;

const MAX_NAME: usize = 64 * 1024;
pub(crate) const PAGE_SIZE: usize = 256;

pub(crate) struct Snapshot {
    directory: tempfile::TempDir,
    data: BufWriter<File>,
    index: BufWriter<File>,
    bytes: u64,
    total: u64,
    skipped: u64,
    path: String,
    filter: pb::DirectoryFilter,
}
impl Snapshot {
    pub(crate) fn new(path: String, filter: pb::DirectoryFilter) -> Result<Self> {
        if path.len() > MAX_NAME {
            bail!("directory path is too long");
        }
        let directory = tempfile::Builder::new()
            .prefix("xfer-directory-")
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let open = |name| {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(directory.path().join(name))
        };
        Ok(Self {
            data: BufWriter::new(open("data")?),
            index: BufWriter::new(open("index")?),
            directory,
            bytes: 0,
            total: 0,
            skipped: 0,
            path,
            filter,
        })
    }
    fn push(&mut self, mut entry: pb::DirectoryEntry) -> Result<()> {
        entry.index = self.total;
        self.push_indexed(entry)
    }
    fn push_indexed(&mut self, entry: pb::DirectoryEntry) -> Result<()> {
        // Backend listings must not inject traversal, terminal controls, or
        // descendants into a picker explicitly restricted to this directory.
        if entry.name.is_empty()
            || matches!(entry.name.as_str(), "." | "..")
            || entry.name.contains('/')
            || entry.name.chars().any(char::is_control)
            || entry.name.len() > MAX_NAME
        {
            self.skipped = self.skipped.saturating_add(1);
            return Ok(());
        }
        if (self.filter == pb::DirectoryFilter::Directories && !entry.directory)
            || (self.filter == pb::DirectoryFilter::RegularFiles && !entry.regular_file)
        {
            return Ok(());
        }
        let body = entry.encode_to_vec();
        self.index.write_all(&self.bytes.to_be_bytes())?;
        self.data.write_all(&(body.len() as u32).to_be_bytes())?;
        self.data.write_all(&body)?;
        self.bytes = self
            .bytes
            .checked_add(body.len() as u64 + 4)
            .ok_or_else(|| anyhow::anyhow!("directory snapshot size overflow"))?;
        self.total += 1;
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> Result<()> {
        self.data.flush()?;
        self.index.flush()?;
        Ok(())
    }
    pub(crate) fn page(&self, offset: u64, limit: u32) -> Result<pb::DirectoryPage> {
        if offset > self.total {
            bail!("directory offset exceeds snapshot");
        }
        let mut page = pb::DirectoryPage {
            entries: Vec::new(),
            next_offset: offset,
            eof: offset == self.total,
            total: self.total,
            skipped: self.skipped,
            path: self.path.clone(),
        };
        if page.eof {
            return Ok(page);
        }
        let mut index = File::open(self.directory.path().join("index"))?;
        index.seek(SeekFrom::Start(
            offset
                .checked_mul(8)
                .ok_or_else(|| anyhow::anyhow!("directory index overflow"))?,
        ))?;
        let mut position = [0; 8];
        index.read_exact(&mut position)?;
        let mut data = File::open(self.directory.path().join("data"))?;
        data.seek(SeekFrom::Start(u64::from_be_bytes(position)))?;
        let limit = if limit == 0 {
            PAGE_SIZE
        } else {
            (limit as usize).min(PAGE_SIZE)
        };
        let mut bytes = self.path.len() + 128;
        while page.entries.len() < limit && page.next_offset < self.total {
            let entry: pb::DirectoryEntry = ipc::read_frame(&mut data)?
                .ok_or_else(|| anyhow::anyhow!("truncated directory snapshot"))?;
            let size = entry.encoded_len() + 16;
            if bytes + size > ipc::MAX_FRAME - 128 {
                break;
            }
            bytes += size;
            page.entries.push(entry);
            page.next_offset += 1;
        }
        page.eof = page.next_offset == self.total;
        Ok(page)
    }
    pub(crate) fn alphabetical(
        &self,
    ) -> Result<impl std::future::Future<Output = Result<Self>> + Send + 'static> {
        self.ordered(None, pb::DirectorySort::NameAsc, true)
    }
    pub(crate) fn filtered(
        &self,
        regexp: regex::Regex,
        order: pb::DirectorySort,
    ) -> Result<impl std::future::Future<Output = Result<Self>> + Send + 'static> {
        self.ordered(Some(regexp), order, false)
    }
    fn ordered(
        &self,
        regexp: Option<regex::Regex>,
        order: pb::DirectorySort,
        reindex: bool,
    ) -> Result<impl std::future::Future<Output = Result<Self>> + Send + 'static> {
        let input = File::open(self.directory.path().join("data"))?;
        let path = self.path.clone();
        let skipped = self.skipped;
        Ok(sort::build(input, path, skipped, regexp, order, reindex))
    }
    pub(crate) async fn local(path: &Path, filter: pb::DirectoryFilter) -> Result<Self> {
        let mut snapshot = Self::new(
            path.to_str()
                .ok_or_else(|| anyhow::anyhow!("Local CWD is not UTF-8"))?
                .into(),
            filter,
        )?;
        for (index, entry) in fs::read_dir(path)?.enumerate() {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(name) = name.to_str() {
                let kind = entry.file_type()?;
                snapshot.push(pb::DirectoryEntry {
                    name: name.into(),
                    directory: kind.is_dir() || (kind.is_symlink() && entry.path().is_dir()),
                    regular_file: kind.is_file(),
                    index: 0,
                    size: if kind.is_file() {
                        entry
                            .metadata()
                            .ok()
                            .filter(|meta| meta.is_file())
                            .map(|meta| meta.len())
                    } else {
                        None
                    },
                })?;
            } else {
                snapshot.skipped = snapshot.skipped.saturating_add(1);
            }
            if index % 256 == 0 {
                tokio::task::yield_now().await;
            }
        }
        snapshot.finish()?;
        Ok(snapshot)
    }
}
impl ListingSink for Snapshot {
    fn emit(&mut self, content: Content) -> Result<()> {
        let (name, directory, regular_file, size) = match content {
            Content::Directory(info) => (
                info.name.trim_end_matches('/').to_owned(),
                true,
                false,
                None,
            ),
            Content::Bucket(info) => (info.name, true, false, None),
            Content::File(info) => {
                let regular = info
                    .unix
                    .and_then(|v| v.mode)
                    .is_none_or(|mode| mode & 0o170000 == 0o100000);
                (
                    info.name,
                    false,
                    regular,
                    if regular { info.size } else { None },
                )
            }
            Content::Comment(_) => return Ok(()),
        };
        self.push(pb::DirectoryEntry {
            name,
            directory,
            regular_file,
            index: 0,
            size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pages_are_indexed_bounded_and_private_and_survive_large_listings() {
        let mut snapshot = Snapshot::new("/test".into(), pb::DirectoryFilter::All).unwrap();
        let path = snapshot.directory.path().to_owned();
        for index in 0..25_000 {
            snapshot
                .push(pb::DirectoryEntry {
                    name: format!("file{index}"),
                    directory: false,
                    regular_file: true,
                    index: 0,
                    size: None,
                })
                .unwrap();
        }
        snapshot.finish().unwrap();
        let page = snapshot.page(24_990, u32::MAX).unwrap();
        assert_eq!(page.entries.len(), 10);
        assert_eq!(page.entries[0].name, "file24990");
        assert!(page.eof);
        assert_eq!(snapshot.page(0, u32::MAX).unwrap().entries.len(), PAGE_SIZE);
        assert!(snapshot.page(25_001, 1).is_err());
        assert!(snapshot.page(25_000, 1).unwrap().eof);
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(path.join("index"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(path.join("data"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        drop(snapshot);
        assert!(!path.exists());
    }
    #[tokio::test]
    async fn local_snapshot_includes_hidden_names_and_filters_without_following_file_links() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(".hidden"), b"").unwrap();
        fs::write(root.path().join("日本語 'file'"), b"").unwrap();
        fs::write(root.path().join("unsafe\nname"), b"").unwrap();
        fs::create_dir(root.path().join("folder")).unwrap();
        std::os::unix::fs::symlink(".hidden", root.path().join("link")).unwrap();
        std::os::unix::fs::symlink("folder", root.path().join("dirlink")).unwrap();
        let snapshot = Snapshot::local(root.path(), pb::DirectoryFilter::RegularFiles)
            .await
            .unwrap();
        let page = snapshot.page(0, 0).unwrap();
        let mut names = page.entries.into_iter().map(|v| v.name).collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, [".hidden", "日本語 'file'"]);
        assert_eq!(page.skipped, 1);
        let snapshot = Snapshot::local(root.path(), pb::DirectoryFilter::Directories)
            .await
            .unwrap();
        assert_eq!(snapshot.page(0, 0).unwrap().total, 2);
        let snapshot = Snapshot::local(root.path(), pb::DirectoryFilter::All)
            .await
            .unwrap();
        assert_eq!(snapshot.page(0, 0).unwrap().total, 5);
    }
    #[test]
    fn unsafe_remote_names_and_descendants_are_not_selectable() {
        let mut snapshot = Snapshot::new("/".into(), pb::DirectoryFilter::All).unwrap();
        for name in [".", "..", "a/b", "\x1b[2J", ""] {
            snapshot
                .push(pb::DirectoryEntry {
                    name: name.into(),
                    directory: false,
                    regular_file: true,
                    index: 0,
                    size: None,
                })
                .unwrap();
        }
        snapshot.finish().unwrap();
        let page = snapshot.page(0, 1).unwrap();
        assert!(page.eof);
        assert_eq!(page.skipped, 5);
    }
    #[test]
    fn long_names_shorten_pages_to_the_frame_budget_without_losing_entries() {
        let mut snapshot = Snapshot::new("/".into(), pb::DirectoryFilter::All).unwrap();
        for index in 0..40 {
            snapshot
                .push(pb::DirectoryEntry {
                    name: format!("{}{index}", "x".repeat(MAX_NAME - 16)),
                    directory: false,
                    regular_file: true,
                    index: 0,
                    size: None,
                })
                .unwrap();
        }
        snapshot.finish().unwrap();
        let mut offset = 0;
        loop {
            let page = snapshot.page(offset, 256).unwrap();
            assert!(!page.entries.is_empty());
            assert!(page.entries.len() < 40);
            assert!(page.encoded_len() + 128 < ipc::MAX_FRAME);
            offset = page.next_offset;
            if page.eof {
                break;
            }
        }
        assert_eq!(offset, 40);
    }
    #[tokio::test]
    async fn regexp_views_stream_from_snapshot_and_preserve_original_indices() {
        let mut snapshot = Snapshot::new("/snapshot/".into(), pb::DirectoryFilter::All).unwrap();
        for name in ["first", "日本語.txt", "middle", "last.txt"] {
            snapshot
                .push(pb::DirectoryEntry {
                    name: name.into(),
                    regular_file: true,
                    ..Default::default()
                })
                .unwrap();
        }
        snapshot.finish().unwrap();
        let regexp = regex::Regex::new(r"\.txt$").unwrap();
        let future = snapshot
            .filtered(regexp, pb::DirectorySort::NameAsc)
            .unwrap();
        // The open input inode remains readable even if the source job closes.
        drop(snapshot);
        let view = future.await.unwrap();
        let page = view.page(0, 256).unwrap();
        assert_eq!(page.total, 2);
        assert_eq!(
            page.entries.iter().map(|v| v.index).collect::<Vec<_>>(),
            [3, 1]
        );
        assert_eq!(page.entries[0].name, "last.txt");
        assert_eq!(view.page(1, 1).unwrap().entries[0].index, 1);
        let empty = view
            .filtered(
                regex::Regex::new("^missing$").unwrap(),
                pb::DirectorySort::NameAsc,
            )
            .unwrap()
            .await
            .unwrap();
        assert_eq!(empty.page(0, 0).unwrap().total, 0);
    }
    #[tokio::test]
    async fn sorting_defaults_to_names_and_sizes_keep_unknowns_last_with_stable_ids() {
        let mut source = Snapshot::new("/".into(), pb::DirectoryFilter::All).unwrap();
        for (name, size, directory) in [
            ("z", Some(5), false),
            ("b", Some(1), false),
            ("a", Some(1), false),
            ("dir", None, true),
            ("unknown", None, false),
            ("zero", Some(0), false),
        ] {
            source
                .push(pb::DirectoryEntry {
                    name: name.into(),
                    size,
                    directory,
                    regular_file: !directory,
                    ..Default::default()
                })
                .unwrap();
        }
        source.finish().unwrap();
        let base = source.alphabetical().unwrap().await.unwrap();
        let entries = base.page(0, 0).unwrap().entries;
        assert_eq!(
            entries.iter().map(|v| v.name.as_str()).collect::<Vec<_>>(),
            ["a", "b", "dir", "unknown", "z", "zero"]
        );
        assert_eq!(
            entries.iter().map(|v| v.index).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4, 5]
        );
        let everything = regex::Regex::new(".*").unwrap();
        for (order, expected) in [
            (
                pb::DirectorySort::NameAsc,
                vec!["a", "b", "dir", "unknown", "z", "zero"],
            ),
            (
                pb::DirectorySort::NameDesc,
                vec!["zero", "z", "unknown", "dir", "b", "a"],
            ),
            (
                pb::DirectorySort::SizeDesc,
                vec!["z", "a", "b", "zero", "dir", "unknown"],
            ),
            (
                pb::DirectorySort::SizeAsc,
                vec!["zero", "a", "b", "z", "dir", "unknown"],
            ),
        ] {
            let view = base
                .filtered(everything.clone(), order)
                .unwrap()
                .await
                .unwrap();
            let page = view.page(0, 0).unwrap();
            assert_eq!(
                page.entries
                    .iter()
                    .map(|v| v.name.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
            for entry in &page.entries {
                let original = entries.iter().find(|v| v.name == entry.name).unwrap();
                assert_eq!(entry.index, original.index);
                assert_eq!(entry.size, original.size);
            }
            assert_eq!(
                fs::read_dir(view.directory.path()).unwrap().count(),
                2,
                "sort runs leaked"
            );
        }
    }
    #[tokio::test]
    async fn external_sort_merges_multiple_runs_and_preserves_every_entry_and_size() {
        let mut source = Snapshot::new("/large/".into(), pb::DirectoryFilter::All).unwrap();
        for index in (0..7000).rev() {
            source
                .push(pb::DirectoryEntry {
                    name: format!("file{index:05}"),
                    size: Some(index),
                    regular_file: true,
                    ..Default::default()
                })
                .unwrap();
        }
        source.finish().unwrap();
        let base = source.alphabetical().unwrap().await.unwrap();
        let desc = base
            .filtered(
                regex::Regex::new(".*").unwrap(),
                pb::DirectorySort::SizeDesc,
            )
            .unwrap()
            .await
            .unwrap();
        let mut offset = 0;
        loop {
            let page = desc.page(offset, 256).unwrap();
            for (position, entry) in page.entries.iter().enumerate() {
                let expected = 6999 - offset - position as u64;
                assert_eq!(entry.name, format!("file{expected:05}"));
                assert_eq!(entry.index, expected);
                assert_eq!(entry.size, Some(expected));
            }
            offset = page.next_offset;
            if page.eof {
                break;
            }
        }
        assert_eq!(offset, 7000);
        assert_eq!(fs::read_dir(desc.directory.path()).unwrap().count(), 2);
    }
    #[tokio::test]
    async fn external_sort_also_spills_on_byte_budget_and_local_sizes_do_not_follow_links() {
        let mut source = Snapshot::new("/long/".into(), pb::DirectoryFilter::All).unwrap();
        for index in (0..40).rev() {
            source
                .push(pb::DirectoryEntry {
                    name: format!("{index:03}{}", "x".repeat(MAX_NAME - 16)),
                    regular_file: true,
                    ..Default::default()
                })
                .unwrap();
        }
        source.finish().unwrap();
        let sorted = source.alphabetical().unwrap().await.unwrap();
        let mut offset = 0;
        loop {
            let page = sorted.page(offset, 256).unwrap();
            for (position, entry) in page.entries.iter().enumerate() {
                assert!(entry
                    .name
                    .starts_with(&format!("{:03}", offset + position as u64)));
            }
            offset = page.next_offset;
            if page.eof {
                break;
            }
        }
        assert_eq!(offset, 40);
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file"), b"abc").unwrap();
        fs::create_dir(root.path().join("dir")).unwrap();
        std::os::unix::fs::symlink("file", root.path().join("link")).unwrap();
        let local = Snapshot::local(root.path(), pb::DirectoryFilter::All)
            .await
            .unwrap();
        let entries = local.page(0, 0).unwrap().entries;
        assert_eq!(
            entries.iter().find(|v| v.name == "file").unwrap().size,
            Some(3)
        );
        assert_eq!(
            entries.iter().find(|v| v.name == "link").unwrap().size,
            None
        );
        assert_eq!(entries.iter().find(|v| v.name == "dir").unwrap().size, None);
    }
}

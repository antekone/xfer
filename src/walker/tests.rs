/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

use super::*;
use std::{collections::HashMap, sync::Mutex};

fn file(name: &str, size: u64) -> Content {
    Content::File(FileInfo {
        name: name.into(),
        size: Some(size),
        modified: None,
        unix: None,
    })
}
fn dir(name: &str) -> Content {
    Content::Directory(DirectoryInfo {
        name: name.into(),
        size: None,
        modified: None,
        unix: None,
    })
}
struct Tree {
    entries: HashMap<String, Vec<Content>>,
    calls: Mutex<Vec<String>>,
}
#[async_trait]
impl WalkSource for Tree {
    async fn select(&self, path: &str) -> Result<Selection> {
        self.calls.lock().unwrap().push(path.into());
        self.entries
            .get(path)
            .cloned()
            .map(Selection::Directory)
            .ok_or_else(|| permanent(format!("unexpected listing: {path}")))
    }
}
#[derive(Default)]
struct Recorder {
    events: Vec<(WalkEvent, String, String)>,
    skip: Option<String>,
}
#[async_trait]
impl WalkVisitor for Recorder {
    async fn visit(&mut self, event: WalkEvent, entry: &WalkEntry) -> Result<WalkControl> {
        self.events
            .push((event, entry.path.clone(), entry.relative.clone()));
        Ok(
            if event == WalkEvent::EnterDirectory && self.skip.as_deref() == Some(&entry.relative) {
                WalkControl::SkipDirectory
            } else {
                WalkControl::Continue
            },
        )
    }
}
#[tokio::test]
async fn walks_depth_first_with_balanced_directory_events_and_skips_symlinks() {
    let mut link = dir("loop/");
    if let Content::Directory(dir) = &mut link {
        dir.unix = Some(UnixMetadata {
            mode: Some(0o120777),
            ..Default::default()
        });
    }
    let tree = Tree {
        entries: HashMap::from([
            (
                "/root/".into(),
                vec![dir("sub/"), link, dir("."), dir(".."), file("a", 1)],
            ),
            ("/root/sub/".into(), vec![file("b", 2)]),
        ]),
        calls: Mutex::default(),
    };
    let mut visitor = Recorder::default();
    walk(&tree, "/root/", &mut visitor).await.unwrap();
    use WalkEvent::*;
    assert_eq!(
        visitor
            .events
            .iter()
            .map(|(event, _, relative)| (*event, relative.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (EnterDirectory, ""),
            (EnterDirectory, "sub"),
            (File, "sub/b"),
            (LeaveDirectory, "sub"),
            (File, "loop"),
            (File, "a"),
            (LeaveDirectory, ""),
        ]
    );
    assert_eq!(visitor.events[2].1, "/root/sub/b");
    assert_eq!(*tree.calls.lock().unwrap(), ["/root/", "/root/sub/"]);
}
#[tokio::test]
async fn visitor_can_prune_before_listing_children() {
    let tree = Tree {
        entries: HashMap::from([("/root".into(), vec![dir("prune")])]),
        calls: Mutex::default(),
    };
    let mut visitor = Recorder {
        skip: Some("prune".into()),
        ..Default::default()
    };
    walk(&tree, "/root", &mut visitor).await.unwrap();
    assert_eq!(tree.calls.lock().unwrap().len(), 1);
    assert_eq!(visitor.events.len(), 3);
}
#[tokio::test]
async fn invalid_child_paths_are_rejected() {
    let tree = Tree {
        entries: HashMap::from([("/root".into(), vec![dir("../escape")])]),
        calls: Mutex::default(),
    };
    let error = walk(&tree, "/root", &mut Recorder::default())
        .await
        .unwrap_err();
    assert!(error.is::<NonRetryableError>());
    assert_eq!(tree.calls.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn local_source_visits_empty_directories_files_and_links() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("empty")).unwrap();
    fs::write(root.path().join("data"), b"abc").unwrap();
    std::os::unix::fs::symlink(root.path(), root.path().join("loop")).unwrap();
    let mut visitor = Recorder::default();
    walk(&LocalSource, root.path().to_str().unwrap(), &mut visitor)
        .await
        .unwrap();
    assert_eq!(visitor.events.len(), 6); // Root + empty directory enter/leave, two files.
    assert!(visitor
        .events
        .iter()
        .any(|(event, _, relative)| *event == WalkEvent::File && relative == "loop"));
    let mut file_visitor = Recorder::default();
    walk(
        &LocalSource,
        root.path().join("data").to_str().unwrap(),
        &mut file_visitor,
    )
    .await
    .unwrap();
    assert_eq!(file_visitor.events.len(), 1);
    assert_eq!(file_visitor.events[0].0, WalkEvent::File);
}
#[tokio::test]
async fn remote_source_respects_explicit_file_listing() {
    struct Remote;
    #[async_trait]
    impl Connection for Remote {
        fn protocol(&self) -> &'static str {
            "mock"
        }
        async fn list(&self, path: &str, sink: &mut dyn ListingSink) -> Result<u64> {
            assert_eq!(path, "/file");
            sink.emit_file(file("file", 3))?;
            Ok(1)
        }
    }
    let mut visitor = Recorder::default();
    walk(&RemoteSource(&Remote), "/file", &mut visitor)
        .await
        .unwrap();
    assert_eq!(
        visitor.events,
        [(WalkEvent::File, "/file".into(), "".into())]
    );
}
#[tokio::test]
async fn callback_errors_stop_the_walk() {
    struct Failure;
    #[async_trait]
    impl WalkVisitor for Failure {
        async fn visit(&mut self, _: WalkEvent, _: &WalkEntry) -> Result<WalkControl> {
            Err(permanent("callback failed"))
        }
    }
    let tree = Tree {
        entries: HashMap::from([("/root".into(), vec![dir("child")])]),
        calls: Mutex::default(),
    };
    assert_eq!(
        walk(&tree, "/root", &mut Failure)
            .await
            .unwrap_err()
            .to_string(),
        "callback failed"
    );
    assert_eq!(tree.calls.lock().unwrap().len(), 1);
}

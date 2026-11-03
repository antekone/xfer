/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Reusable depth-first directory traversal. Sources provide immediate children;
//! async visitors receive pre/post directory events and file events. No mutations
//! or retries are performed here: callers own those policies.
use crate::{
    backend::Connection,
    error::NonRetryableError,
    listing::{Content, DirectoryInfo, FileInfo, ListingSink, UnixMetadata},
};
use anyhow::Result;
use async_trait::async_trait;
use std::{fs, os::unix::fs::MetadataExt, path::Path};

/// A file selected directly, or the immediate contents of a directory.
pub enum Selection {
    File(Content),
    Directory(Vec<Content>),
}

#[async_trait]
pub trait WalkSource: Send + Sync {
    async fn select(&self, path: &str) -> Result<Selection>;
}

pub struct RemoteSource<'a>(pub &'a dyn Connection);
#[derive(Default)]
struct Collect {
    entries: Vec<Content>,
    file: Option<Content>,
}
impl ListingSink for Collect {
    fn emit(&mut self, content: Content) -> Result<()> {
        self.entries.push(content);
        Ok(())
    }
    fn emit_file(&mut self, content: Content) -> Result<()> {
        self.file = Some(content);
        Ok(())
    }
}
#[async_trait]
impl WalkSource for RemoteSource<'_> {
    async fn select(&self, path: &str) -> Result<Selection> {
        let mut sink = Collect::default();
        self.0.list(path, &mut sink).await?;
        Ok(match sink.file {
            Some(file) => Selection::File(file),
            None => Selection::Directory(sink.entries),
        })
    }
}

/// Local source for future reverse-mirror traversal. Descendant symlinks are
/// file events, never followed, just as for remote traversal.
pub struct LocalSource;
fn local_record(path: &Path, name: String) -> Result<Content> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| permanent(format!("local path {}: {error}", path.display())))?;
    let modified = metadata.modified().ok().map(chrono::DateTime::from);
    let unix = Some(UnixMetadata {
        mode: Some(metadata.mode()),
        uid: Some(metadata.uid()),
        gid: Some(metadata.gid()),
        ..UnixMetadata::default()
    });
    Ok(if metadata.is_dir() {
        Content::Directory(DirectoryInfo {
            name,
            modified,
            size: Some(metadata.len()),
            unix,
        })
    } else {
        Content::File(FileInfo {
            name,
            modified,
            size: Some(metadata.len()),
            unix,
        })
    })
}
#[async_trait]
impl WalkSource for LocalSource {
    async fn select(&self, path: &str) -> Result<Selection> {
        let path = Path::new(path);
        let record = local_record(path, path.to_string_lossy().into_owned())?;
        if !matches!(record, Content::Directory(_)) {
            return Ok(Selection::File(record));
        }
        let mut entries = Vec::new();
        for entry in fs::read_dir(path)
            .map_err(|error| permanent(format!("local directory {}: {error}", path.display())))?
        {
            let entry = entry.map_err(|error| permanent(error.to_string()))?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| permanent("non-UTF-8 local filenames are not supported"))?;
            entries.push(local_record(&entry.path(), name)?);
        }
        Ok(Selection::Directory(entries))
    }
}

#[derive(Clone, Debug)]
pub struct WalkEntry {
    /// Resolved source path; suitable for subsequent backend operations.
    pub path: String,
    /// Path relative to the selected root (empty for the root itself).
    pub relative: String,
    /// Root directory metadata may be unavailable from a listing-only source.
    pub content: Content,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalkEvent {
    EnterDirectory,
    File,
    LeaveDirectory,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalkControl {
    Continue,
    /// Only meaningful on EnterDirectory; no descendants or leave event follow.
    SkipDirectory,
}
#[async_trait]
pub trait WalkVisitor: Send {
    async fn visit(&mut self, event: WalkEvent, entry: &WalkEntry) -> Result<WalkControl>;
}
fn permanent(message: impl Into<String>) -> anyhow::Error {
    NonRetryableError(message.into()).into()
}
fn name(content: &Content) -> Option<&str> {
    match content {
        Content::File(file) => Some(&file.name),
        Content::Directory(dir) => Some(dir.name.trim_end_matches('/')),
        Content::Bucket(bucket) => Some(&bucket.name),
        Content::Comment(_) => None,
    }
}
fn is_directory(content: &Content) -> bool {
    match content {
        // SFTP classifies symlinks to directories as Directory for completion;
        // keep their own metadata and treat them as leaves to avoid cycles.
        Content::Directory(dir) => !dir
            .unix
            .as_ref()
            .and_then(|unix| unix.mode)
            .is_some_and(|mode| mode & 0o170000 == 0o120000),
        Content::Bucket(_) => true,
        _ => false,
    }
}
fn join(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.into()
    } else {
        format!("{}/{name}", parent.trim_end_matches('/'))
    }
}
struct Frame {
    entry: WalkEntry,
    children: std::vec::IntoIter<Content>,
}
/// Walk a resolved root without changing the working directory. Traversal is
/// iterative (no recursion limit); only active directory listings are retained.
/// Callback/source failures stop immediately. Dropping the future cancels it.
pub async fn walk(
    source: &dyn WalkSource,
    root: &str,
    visitor: &mut dyn WalkVisitor,
) -> Result<()> {
    let selection = source.select(root).await?;
    let children = match selection {
        Selection::File(content) => {
            visitor
                .visit(
                    WalkEvent::File,
                    &WalkEntry {
                        path: root.into(),
                        relative: String::new(),
                        content,
                    },
                )
                .await?;
            return Ok(());
        }
        Selection::Directory(children) => children,
    };
    let root = WalkEntry {
        path: root.into(),
        relative: String::new(),
        content: Content::Directory(DirectoryInfo {
            name: root.into(),
            modified: None,
            size: None,
            unix: None,
        }),
    };
    if visitor.visit(WalkEvent::EnterDirectory, &root).await? == WalkControl::SkipDirectory {
        return Ok(());
    }
    let mut stack = vec![Frame {
        entry: root,
        children: children.into_iter(),
    }];
    while let Some(frame) = stack.last_mut() {
        let Some(content) = frame.children.next() else {
            let frame = stack.pop().unwrap();
            visitor
                .visit(WalkEvent::LeaveDirectory, &frame.entry)
                .await?;
            continue;
        };
        let Some(name) = name(&content) else {
            continue;
        };
        if matches!(name, "." | "..") {
            continue;
        }
        if name.is_empty() || name.contains('/') || name.contains('\0') {
            return Err(permanent("directory source returned an invalid child name"));
        }
        let entry = WalkEntry {
            path: join(&frame.entry.path, name),
            relative: join(&frame.entry.relative, name),
            content,
        };
        if !is_directory(&entry.content) {
            visitor.visit(WalkEvent::File, &entry).await?;
        } else if visitor.visit(WalkEvent::EnterDirectory, &entry).await? == WalkControl::Continue {
            // Force directory semantics, e.g. for S3 keys sharing a prefix name.
            let children = match source.select(&format!("{}/", entry.path)).await? {
                Selection::Directory(children) => children,
                Selection::File(_) => {
                    return Err(permanent("directory changed to a file during traversal"))
                }
            };
            stack.push(Frame {
                entry,
                children: children.into_iter(),
            });
        }
        // Even an in-memory tree must remain cooperatively cancellable.
        tokio::task::yield_now().await;
    }
    Ok(())
}

#[cfg(test)]
mod tests;

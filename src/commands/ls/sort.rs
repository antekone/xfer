/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Bounded-record-memory external merge sort. Small listings stay in memory;
//! larger ones use private temporary runs, removed on success/error/cancellation.
use super::{attributes, LsArgs};
use crate::{
    core::OutputStore,
    error::NonRetryableError,
    listing::{render_with_options, Content, ListingSink},
};
use anyhow::Result;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

const MAX_ENTRIES: usize = 2048;
const MAX_BYTES: usize = 1024 * 1024;

fn local_error(error: anyhow::Error) -> anyhow::Error {
    NonRetryableError(format!("cannot sort/write listing: {error}")).into()
}

pub(super) struct SortedListing {
    args: LsArgs,
    buffer: Vec<Content>,
    bytes: usize,
    limit: usize,
    directory: Option<tempfile::TempDir>,
    runs: usize,
}
impl SortedListing {
    pub(super) fn new(args: LsArgs) -> Self {
        Self {
            args,
            buffer: Vec::new(),
            bytes: 0,
            limit: MAX_ENTRIES,
            directory: None,
            runs: 0,
        }
    }
    fn push(&mut self, entry: Content) -> Result<()> {
        self.bytes += attributes(&entry).0.len() + 128;
        self.buffer.push(entry);
        if self.buffer.len() >= self.limit || self.bytes >= MAX_BYTES {
            self.spill()?;
        }
        Ok(())
    }
    fn spill(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort_unstable_by(|a, b| self.args.compare(a, b));
        if self.directory.is_none() {
            self.directory = Some(
                tempfile::Builder::new()
                    .prefix("xfer-ls-")
                    .permissions(fs::Permissions::from_mode(0o700))
                    .tempdir()?,
            );
        }
        let path = self
            .directory
            .as_ref()
            .unwrap()
            .path()
            .join(self.runs.to_string());
        let mut writer = new_run(&path)?;
        for entry in self.buffer.drain(..) {
            write_record(&mut writer, &entry)?;
        }
        writer.flush()?;
        self.runs += 1;
        self.bytes = 0;
        Ok(())
    }
    pub(super) async fn write_to(mut self, output: &mut OutputStore) -> Result<()> {
        self.write_sorted(output).await.map_err(local_error)
    }
    async fn write_sorted(&mut self, output: &mut OutputStore) -> Result<()> {
        if self.runs == 0 {
            self.buffer.sort_unstable_by(|a, b| self.args.compare(a, b));
            for (index, entry) in self.buffer.iter().enumerate() {
                output.write_line(&render_with_options(entry, self.args.display))?;
                if index % 256 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            return Ok(());
        }
        self.spill()?;
        let directory = self.directory.as_ref().unwrap().path();
        let (mut first, mut count, mut next) = (0, self.runs, self.runs);
        // Numbered runs avoid retaining a growing list of filenames or open
        // descriptors. Each merge holds two records/readers and one writer.
        while count > 1 {
            let new_first = next;
            for index in (first..first + count).step_by(2) {
                let left = directory.join(index.to_string());
                let destination = directory.join(next.to_string());
                if index + 1 == first + count {
                    fs::rename(left, destination)?;
                } else {
                    let right = directory.join((index + 1).to_string());
                    merge(&left, &right, &destination, &self.args).await?;
                    fs::remove_file(left)?;
                    fs::remove_file(right)?;
                }
                next += 1;
            }
            first = new_first;
            count = count.div_ceil(2);
        }
        let mut reader = BufReader::new(File::open(directory.join(first.to_string()))?);
        let mut line = String::new();
        let mut count = 0;
        while let Some(entry) = read_record(&mut reader, &mut line)? {
            output.write_line(&render_with_options(&entry, self.args.display))?;
            count += 1;
            if count % 256 == 0 {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }
}
impl ListingSink for SortedListing {
    fn emit(&mut self, entry: Content) -> Result<()> {
        if !self.args.all
            && !matches!(entry, Content::Comment(_))
            && attributes(&entry).0.starts_with('.')
        {
            return Ok(());
        }
        self.push(entry).map_err(local_error)
    }
    fn emit_file(&mut self, entry: Content) -> Result<()> {
        self.push(entry).map_err(local_error)
    }
}

fn new_run(path: &Path) -> Result<BufWriter<File>> {
    Ok(BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?,
    ))
}
fn write_record(writer: &mut impl Write, entry: &Content) -> Result<()> {
    serde_json::to_writer(&mut *writer, entry)?;
    writer.write_all(b"\n")?;
    Ok(())
}
fn read_record(reader: &mut impl BufRead, line: &mut String) -> Result<Option<Content>> {
    line.clear();
    if reader.read_line(line)? == 0 {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(line)?))
}
async fn merge(left: &Path, right: &Path, destination: &Path, args: &LsArgs) -> Result<()> {
    let mut left = BufReader::new(File::open(left)?);
    let mut right = BufReader::new(File::open(right)?);
    let (mut left_line, mut right_line) = (String::new(), String::new());
    let mut a = read_record(&mut left, &mut left_line)?;
    let mut b = read_record(&mut right, &mut right_line)?;
    let mut writer = new_run(destination)?;
    let mut count = 0;
    while a.is_some() || b.is_some() {
        let take_left = match (&a, &b) {
            (Some(a), Some(b)) => !args.compare(a, b).is_gt(),
            (Some(_), None) => true,
            _ => false,
        };
        if take_left {
            write_record(&mut writer, a.as_ref().unwrap())?;
            a = read_record(&mut left, &mut left_line)?;
        } else {
            write_record(&mut writer, b.as_ref().unwrap())?;
            b = read_record(&mut right, &mut right_line)?;
        }
        count += 1;
        if count % 256 == 0 {
            tokio::task::yield_now().await;
        }
    }
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{directory, entries, file};
    use super::*;

    fn output() -> OutputStore {
        OutputStore::new(888888)
    }

    #[tokio::test]
    async fn memory_and_disk_sorting_match_for_all_options_and_hide_dotfiles_only_in_views() {
        for flags in ["", "-a", "-F", "-l", "-lF", "-S", "-r", "-Sr", "-laFSr"] {
            let args = LsArgs::parse(flags.split_whitespace()).unwrap();
            let mut outputs = Vec::new();
            for limit in [1, 2, 3, 4, 100] {
                let mut listing = SortedListing::new(args.clone());
                listing.limit = limit;
                for entry in entries() {
                    listing.emit(entry).unwrap();
                }
                assert!(listing.buffer.len() < limit);
                let directory = listing.directory.as_ref().map(|dir| dir.path().to_owned());
                if let Some(path) = &directory {
                    assert_eq!(fs::metadata(path).unwrap().permissions().mode() & 0o077, 0);
                    assert_eq!(
                        fs::metadata(path.join("0")).unwrap().permissions().mode() & 0o077,
                        0
                    );
                }
                let mut output = output();
                listing.write_to(&mut output).await.unwrap();
                outputs.push(output.read_all());
                let _ = fs::remove_file(output.path());
                assert!(directory.is_none_or(|path| !path.exists()));
            }
            assert!(
                outputs.windows(2).all(|pair| pair[0] == pair[1]),
                "{flags}: {outputs:?}"
            );
            let mut expected = entries();
            expected.retain(|entry| args.all || !attributes(entry).0.starts_with('.'));
            expected.sort_unstable_by(|a, b| args.compare(a, b));
            assert_eq!(
                outputs[0],
                expected
                    .iter()
                    .map(|entry| render_with_options(entry, args.display))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn explicit_hidden_files_are_visible_and_names_are_safely_escaped() {
        let mut listing = SortedListing::new(LsArgs::default());
        listing.limit = 1;
        listing.emit(file(".hidden", Some(1))).unwrap();
        listing.emit(directory(".hidden-dir/", None)).unwrap();
        listing.emit_file(file(".requested", Some(1))).unwrap();
        listing.emit(file("résumé\n\x1b[2J.txt", Some(1))).unwrap();
        let mut output = output();
        listing.write_to(&mut output).await.unwrap();
        assert_eq!(
            output.read_all(),
            vec![".requested", "résumé\\n\\u{1b}[2J.txt"]
        );
        fs::remove_file(output.path()).unwrap();
    }

    #[tokio::test]
    async fn large_listings_are_not_truncated_at_the_cache_or_sort_buffer_limits() {
        let mut listing = SortedListing::new(LsArgs::default());
        for index in (0..5130).rev() {
            listing
                .emit(file(&format!("file{index:05}"), Some(index)))
                .unwrap();
            assert!(listing.buffer.len() < MAX_ENTRIES);
        }
        let mut output = output();
        listing.write_to(&mut output).await.unwrap();
        assert_eq!(
            output.read_all(),
            (0..5130)
                .map(|index| format!("file{index:05}"))
                .collect::<Vec<_>>()
        );
        fs::remove_file(output.path()).unwrap();
    }

    #[tokio::test]
    async fn temporary_runs_are_removed_after_errors_and_cancellation() {
        let mut listing = SortedListing::new(LsArgs::default());
        listing.limit = 300;
        for _ in 0..600 {
            listing.emit(file("file", None)).unwrap();
        }
        let directory = listing.directory.as_ref().unwrap().path().to_owned();
        let mut output = output();
        let mut future = Box::pin(listing.write_to(&mut output));
        assert!(futures_util::poll!(&mut future).is_pending());
        drop(future);
        assert!(!directory.exists());
        let mut listing = SortedListing::new(LsArgs::default());
        listing.limit = 1;
        for entry in entries() {
            listing.emit(entry).unwrap();
        }
        let directory = listing.directory.as_ref().unwrap().path().to_owned();
        fs::remove_file(directory.join("0")).unwrap();
        assert!(listing
            .write_to(&mut output)
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
        assert!(!directory.exists());
        let _ = fs::remove_file(output.path());
    }
}

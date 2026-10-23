//! Bounded-memory external merge sort for paged directory snapshots. Runs live
//! inside the result's private directory, so cancellation removes partial work.
use super::Snapshot;
use crate::ipc::{self, pb};
use anyhow::Result;
use prost::Message;
use std::{
    cmp::Ordering,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

const RUN_ENTRIES: usize = 2048;
const RUN_BYTES: usize = 1024 * 1024;

fn compare(a: &pb::DirectoryEntry, b: &pb::DirectoryEntry, order: pb::DirectorySort) -> Ordering {
    use pb::DirectorySort::*;
    let primary = match order {
        NameAsc => a.name.cmp(&b.name),
        NameDesc => b.name.cmp(&a.name),
        SizeAsc | SizeDesc => match (a.size, b.size) {
            (Some(a), Some(b)) if order == SizeAsc => a.cmp(&b),
            (Some(a), Some(b)) => b.cmp(&a),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        },
    };
    primary
        .then_with(|| a.name.cmp(&b.name))
        .then_with(|| b.directory.cmp(&a.directory))
        .then_with(|| a.index.cmp(&b.index))
}
fn writer(path: &Path) -> Result<BufWriter<File>> {
    Ok(BufWriter::new(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)?,
    ))
}
fn record(out: &mut impl Write, entry: &pb::DirectoryEntry) -> Result<()> {
    let body = entry.encode_to_vec();
    out.write_all(&(body.len() as u32).to_be_bytes())?;
    out.write_all(&body)?;
    Ok(())
}
fn spill(
    entries: &mut Vec<pb::DirectoryEntry>,
    directory: &Path,
    number: usize,
    order: pb::DirectorySort,
) -> Result<()> {
    entries.sort_unstable_by(|a, b| compare(a, b, order));
    let mut out = writer(&directory.join(format!("run-{number}")))?;
    for entry in entries.drain(..) {
        record(&mut out, &entry)?;
    }
    out.flush()?;
    Ok(())
}
async fn merge(
    left: &Path,
    right: &Path,
    destination: &Path,
    order: pb::DirectorySort,
) -> Result<()> {
    let mut left = BufReader::new(File::open(left)?);
    let mut right = BufReader::new(File::open(right)?);
    let mut a = ipc::read_frame::<pb::DirectoryEntry>(&mut left)?;
    let mut b = ipc::read_frame::<pb::DirectoryEntry>(&mut right)?;
    let mut out = writer(destination)?;
    let mut count = 0usize;
    while a.is_some() || b.is_some() {
        let take_left = match (&a, &b) {
            (Some(a), Some(b)) => !compare(a, b, order).is_gt(),
            (Some(_), None) => true,
            _ => false,
        };
        if take_left {
            record(&mut out, a.as_ref().unwrap())?;
            a = ipc::read_frame(&mut left)?;
        } else {
            record(&mut out, b.as_ref().unwrap())?;
            b = ipc::read_frame(&mut right)?;
        }
        count += 1;
        if count.is_multiple_of(128) {
            tokio::task::yield_now().await;
        }
    }
    out.flush()?;
    Ok(())
}
fn emit(output: &mut Snapshot, entry: pb::DirectoryEntry, reindex: bool) -> Result<()> {
    if reindex {
        output.push(entry)
    } else {
        output.push_indexed(entry)
    }
}

pub(super) async fn build(
    input: File,
    path: String,
    skipped: u64,
    regexp: Option<regex::Regex>,
    order: pb::DirectorySort,
    reindex: bool,
) -> Result<Snapshot> {
    let mut output = Snapshot::new(path, pb::DirectoryFilter::All)?;
    output.skipped = skipped;
    let mut input = BufReader::new(input);
    let mut buffer = Vec::new();
    let (mut bytes, mut runs, mut count) = (0usize, 0usize, 0usize);
    while let Some(entry) = ipc::read_frame::<pb::DirectoryEntry>(&mut input)? {
        if regexp.as_ref().is_none_or(|re| re.is_match(&entry.name)) {
            bytes += entry.encoded_len() + 128;
            buffer.push(entry);
            if buffer.len() >= RUN_ENTRIES || bytes >= RUN_BYTES {
                spill(&mut buffer, output.directory.path(), runs, order)?;
                runs += 1;
                bytes = 0;
            }
        }
        count += 1;
        if count.is_multiple_of(128) {
            tokio::task::yield_now().await;
        }
    }
    if runs == 0 {
        buffer.sort_unstable_by(|a, b| compare(a, b, order));
        for (index, entry) in buffer.into_iter().enumerate() {
            emit(&mut output, entry, reindex)?;
            if index.is_multiple_of(128) {
                tokio::task::yield_now().await;
            }
        }
    } else {
        if !buffer.is_empty() {
            spill(&mut buffer, output.directory.path(), runs, order)?;
            runs += 1;
        }
        let (mut first, mut count, mut next) = (0, runs, runs);
        // Numbered runs avoid retaining an unbounded path list or file handles.
        while count > 1 {
            let new_first = next;
            for index in (first..first + count).step_by(2) {
                let left = output.directory.path().join(format!("run-{index}"));
                let destination = output.directory.path().join(format!("run-{next}"));
                if index + 1 == first + count {
                    fs::rename(left, destination)?;
                } else {
                    let right = output.directory.path().join(format!("run-{}", index + 1));
                    merge(&left, &right, &destination, order).await?;
                    fs::remove_file(left)?;
                    fs::remove_file(right)?;
                }
                next += 1;
            }
            first = new_first;
            count = count.div_ceil(2);
        }
        let path = output.directory.path().join(format!("run-{first}"));
        let mut sorted = BufReader::new(File::open(&path)?);
        let mut count = 0usize;
        while let Some(entry) = ipc::read_frame::<pb::DirectoryEntry>(&mut sorted)? {
            emit(&mut output, entry, reindex)?;
            count += 1;
            if count.is_multiple_of(128) {
                tokio::task::yield_now().await;
            }
        }
        drop(sorted);
        fs::remove_file(path)?;
    }
    output.finish()?;
    Ok(output)
}

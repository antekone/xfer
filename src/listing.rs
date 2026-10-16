//! Backend-independent listing records and their single default text renderer.
//!
//! Backends emit records incrementally, keeping large/paginated listings bounded.
//! A sink can render them, collect a page, or feed a future completion/picker UI.
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "info", rename_all = "snake_case")]
pub enum Content {
    Directory(DirectoryInfo),
    File(FileInfo),
    Bucket(BucketInfo),
    Comment(CommentInfo),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DirectoryInfo {
    pub name: String,
    /// None for virtual S3 prefixes; not inferred from descendant files.
    pub modified: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub size: Option<u64>,
    pub modified: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    /// Bucket creation date, NOT a last-modification date.
    pub created: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommentInfo {
    pub text: String,
}

pub trait ListingSink: Send {
    fn emit(&mut self, content: Content) -> Result<()>;
}

pub fn render(content: &Content) -> String {
    let (name, date, size, directory) = match content {
        Content::Directory(info) => (&info.name, info.modified, None, true),
        Content::File(info) => (&info.name, info.modified, info.size, false),
        Content::Bucket(info) => (&info.name, info.created, None, true),
        Content::Comment(info) => return format!("# {}", printable(&info.text)),
    };
    let date = date
        .map(|date| date.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("{:>23}", "-"));
    let mut name = printable(name);
    if directory && !name.ends_with('/') {
        name.push('/');
    }
    format!("[{date}] {:>10}  {name}", format_size(size))
}

fn printable(text: &str) -> String {
    let mut output = String::new();
    for ch in text.chars() {
        if ch.is_control() {
            output.extend(ch.escape_default());
        } else {
            output.push(ch);
        }
    }
    output
}

fn format_size(size: Option<u64>) -> String {
    let Some(size) = size else {
        return "-".into();
    };
    if size < 1024 {
        return format!("{size} B");
    }
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut value = size as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < units.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", units[unit])
}

#[cfg(test)]
mod tests {
    use super::*;
    fn date() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-16T13:22:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn dates_sizes_and_names_are_aligned_across_entry_types() {
        let file = render(&Content::File(FileInfo {
            name: "notes.txt".into(),
            size: Some(4096),
            modified: Some(date()),
        }));
        let bucket = render(&Content::Bucket(BucketInfo {
            name: "archive".into(),
            created: Some(date()),
        }));
        let directory = render(&Content::Directory(DirectoryInfo {
            name: "src/".into(),
            modified: None,
        }));
        assert_eq!(file, "[2026-10-16 13:22:00 UTC]    4.0 KiB  notes.txt");
        assert_eq!(file.find("notes.txt"), bucket.find("archive/"));
        assert_eq!(file.find("notes.txt"), directory.find("src/"));
        assert!(!directory.ends_with("src//"));
    }

    #[test]
    fn unknown_metadata_is_not_fabricated() {
        let missing = render(&Content::File(FileInfo {
            name: "unknown".into(),
            size: None,
            modified: None,
        }));
        assert!(!missing.contains("0 B"));
        assert!(missing.contains('-'));
        assert_eq!(format_size(Some(0)), "0 B");
        assert_eq!(format_size(Some(1024)), "1.0 KiB");
        assert_eq!(format_size(Some(1024 * 1024)), "1.0 MiB");
    }

    #[test]
    fn names_are_unicode_readable_but_cannot_inject_terminal_controls() {
        let value = render(&Content::File(FileInfo {
            name: "résumé\n\x1b[2J.txt".into(),
            size: Some(12),
            modified: None,
        }));
        assert!(value.contains("résumé\\n\\u{1b}[2J.txt"));
        assert!(!value.contains('\n'));
        assert!(!value.contains('\x1b'));
    }

    #[test]
    fn comments_are_supported_and_records_are_serializable() {
        let entry = Content::Comment(CommentInfo {
            text: "nothing here".into(),
        });
        assert_eq!(render(&entry), "# nothing here");
        let serialized = serde_json::to_string(&entry).unwrap();
        assert_eq!(serde_json::from_str::<Content>(&serialized).unwrap(), entry);
    }
}

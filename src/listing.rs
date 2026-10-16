//! Backend-independent listing records and text rendering.
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
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unix: Option<UnixMetadata>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub size: Option<u64>,
    pub modified: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unix: Option<UnixMetadata>,
}

/// Numeric server-side ownership and POSIX mode (file type plus permission bits).
/// These IDs must not be resolved using the client's unrelated account database.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnixMetadata {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    /// Best-effort server-provided owner name, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Best-effort server-provided group name, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
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
    /// An explicitly named file, not a child of a directory listing. Display
    /// filters should not hide a dotfile the user requested directly.
    fn emit_file(&mut self, content: Content) -> Result<()> {
        self.emit(content)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DisplayOptions {
    pub long: bool,
    pub classify: bool,
}

/// Legacy long, classified rendering for generic record sinks.
pub fn render(content: &Content) -> String {
    render_with_options(
        content,
        DisplayOptions {
            long: true,
            classify: true,
        },
    )
}

pub fn render_with_options(content: &Content, options: DisplayOptions) -> String {
    let (name, date, size, directory, unix) = match content {
        Content::Directory(info) => (
            &info.name,
            info.modified,
            info.size,
            true,
            info.unix.as_ref(),
        ),
        Content::File(info) => (
            &info.name,
            info.modified,
            info.size,
            false,
            info.unix.as_ref(),
        ),
        Content::Bucket(info) => (&info.name, info.created, None, true, None),
        Content::Comment(info) => return format!("# {}", printable(&info.text)),
    };
    let mut name = printable(if directory {
        name.trim_end_matches('/')
    } else {
        name
    });
    if directory && options.classify {
        name.push('/');
    }
    if !options.long {
        return name;
    }
    let date = date
        .map(|date| date.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("{:>23}", "-"));
    let basic = format!("[{date}] {:>10}  {name}", format_size(size));
    match unix {
        Some(unix) => format!(
            "{} {:>10} {:>10} {basic}",
            format_mode(unix.mode),
            unix.owner
                .as_deref()
                .map(printable)
                .or_else(|| unix.uid.map(|id| id.to_string()))
                .unwrap_or_else(|| "-".into()),
            unix.group
                .as_deref()
                .map(printable)
                .or_else(|| unix.gid.map(|id| id.to_string()))
                .unwrap_or_else(|| "-".into())
        ),
        None => basic,
    }
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
    size.map(|size| size.to_string())
        .unwrap_or_else(|| "-".into())
}

fn format_mode(mode: Option<u32>) -> String {
    let Some(mode) = mode else {
        return "??????????".into();
    };
    let kind = match mode & 0o170000 {
        0o100000 => '-',
        0o040000 => 'd',
        0o120000 => 'l',
        0o010000 => 'p',
        0o140000 => 's',
        0o060000 => 'b',
        0o020000 => 'c',
        _ => '?',
    };
    let mut permissions = vec![kind];
    for shift in [6, 3, 0] {
        permissions.push(if mode & (4 << shift) != 0 { 'r' } else { '-' });
        permissions.push(if mode & (2 << shift) != 0 { 'w' } else { '-' });
        permissions.push(if mode & (1 << shift) != 0 { 'x' } else { '-' });
    }
    for (special, executable, index, lower, upper) in [
        (0o4000, 0o100, 3, 's', 'S'),
        (0o2000, 0o010, 6, 's', 'S'),
        (0o1000, 0o001, 9, 't', 'T'),
    ] {
        if mode & special != 0 {
            permissions[index] = if mode & executable != 0 { lower } else { upper };
        }
    }
    permissions.into_iter().collect()
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
            unix: None,
            name: "notes.txt".into(),
            size: Some(4096),
            modified: Some(date()),
        }));
        let bucket = render(&Content::Bucket(BucketInfo {
            name: "archive".into(),
            created: Some(date()),
        }));
        let directory = render(&Content::Directory(DirectoryInfo {
            unix: None,
            size: None,
            name: "src/".into(),
            modified: None,
        }));
        assert_eq!(file, "[2026-10-16 13:22:00 UTC]       4096  notes.txt");
        assert_eq!(file.find("notes.txt"), bucket.find("archive/"));
        assert_eq!(file.find("notes.txt"), directory.find("src/"));
        assert!(!directory.ends_with("src//"));
    }

    #[test]
    fn bare_names_are_default_and_long_mode_and_directory_markers_are_independent() {
        let directory = Content::Directory(DirectoryInfo {
            name: "src/".into(),
            size: None,
            modified: None,
            unix: None,
        });
        let bucket = Content::Bucket(BucketInfo {
            name: "archive".into(),
            created: None,
        });
        for (entry, name) in [(directory, "src"), (bucket, "archive")] {
            assert_eq!(render_with_options(&entry, DisplayOptions::default()), name);
            assert_eq!(
                render_with_options(
                    &entry,
                    DisplayOptions {
                        classify: true,
                        long: false
                    }
                ),
                format!("{name}/")
            );
            assert_eq!(
                render_with_options(
                    &entry,
                    DisplayOptions {
                        classify: false,
                        long: true
                    }
                ),
                render(&entry).trim_end_matches('/')
            );
            assert_eq!(
                render_with_options(
                    &entry,
                    DisplayOptions {
                        classify: true,
                        long: true
                    }
                ),
                render(&entry)
            );
        }
        let file = Content::File(FileInfo {
            name: "file\n\x1b".into(),
            size: Some(1),
            modified: None,
            unix: None,
        });
        assert_eq!(
            render_with_options(&file, DisplayOptions::default()),
            "file\\n\\u{1b}"
        );
        assert_eq!(
            render_with_options(
                &file,
                DisplayOptions {
                    classify: true,
                    long: false
                }
            ),
            "file\\n\\u{1b}"
        );
    }

    #[test]
    fn server_owner_group_names_override_ids_and_are_sanitized() {
        let entry = Content::File(FileInfo {
            name: "notes.txt".into(),
            size: Some(1),
            modified: None,
            unix: Some(UnixMetadata {
                mode: Some(0o100644),
                uid: Some(1000),
                gid: Some(1001),
                owner: Some("alice".into()),
                group: Some("ops\x1b".into()),
            }),
        });
        let line = render(&entry);
        assert!(line.contains("alice"));
        assert!(line.contains("ops\\u{1b}"));
        assert!(!line.contains('\x1b'));
    }

    #[test]
    fn unknown_metadata_is_not_fabricated() {
        let missing = render(&Content::File(FileInfo {
            unix: None,
            name: "unknown".into(),
            size: None,
            modified: None,
        }));
        assert!(!missing.contains(" 0 "));
        assert!(missing.contains('-'));
        assert_eq!(format_size(Some(0)), "0");
        assert_eq!(format_size(Some(1024)), "1024");
        assert_eq!(format_size(Some(1024 * 1024)), "1048576");
    }

    #[test]
    fn names_are_unicode_readable_but_cannot_inject_terminal_controls() {
        let value = render(&Content::File(FileInfo {
            unix: None,
            name: "résumé\n\x1b[2J.txt".into(),
            size: Some(12),
            modified: None,
        }));
        assert!(value.contains("résumé\\n\\u{1b}[2J.txt"));
        assert!(!value.contains('\n'));
        assert!(!value.contains('\x1b'));
    }

    #[test]
    fn unix_listing_shows_mode_ids_mtime_and_exact_byte_counts() {
        let entry = Content::File(FileInfo {
            name: "notes.txt".into(),
            size: Some(4096),
            modified: Some(date()),
            unix: Some(UnixMetadata {
                mode: Some(0o100640),
                uid: Some(1000),
                gid: Some(1001),
                owner: None,
                group: None,
            }),
        });
        let line = render(&entry);
        assert_eq!(
            line.split_whitespace().collect::<Vec<_>>(),
            vec![
                "-rw-r-----",
                "1000",
                "1001",
                "[2026-10-16",
                "13:22:00",
                "UTC]",
                "4096",
                "notes.txt",
            ]
        );
        assert_eq!(
            serde_json::from_str::<Content>(&serde_json::to_string(&entry).unwrap()).unwrap(),
            entry
        );
        assert_eq!(format_size(Some(u64::MAX)), "18446744073709551615");

        let unknown = render(&Content::File(FileInfo {
            name: "unknown".into(),
            size: None,
            modified: None,
            unix: Some(UnixMetadata::default()),
        }));
        assert!(unknown.starts_with("??????????"));
        assert_eq!(
            unknown.split_whitespace().collect::<Vec<_>>(),
            vec!["??????????", "-", "-", "[", "-]", "-", "unknown"]
        );
    }

    #[test]
    fn unix_permissions_include_file_types_and_special_bits() {
        for (mode, expected) in [
            (0o100644, "-rw-r--r--"),
            (0o040755, "drwxr-xr-x"),
            (0o120777, "lrwxrwxrwx"),
            (0o010600, "prw-------"),
            (0o140600, "srw-------"),
            (0o060600, "brw-------"),
            (0o020600, "crw-------"),
            (0o100000, "----------"),
            (0o104755, "-rwsr-xr-x"),
            (0o104644, "-rwSr--r--"),
            (0o102750, "-rwxr-s---"),
            (0o041777, "drwxrwxrwt"),
            (0o041776, "drwxrwxrwT"),
        ] {
            assert_eq!(format_mode(Some(mode)), expected);
        }
        assert_eq!(format_mode(None), "??????????");
    }

    #[test]
    fn old_records_without_unix_metadata_still_deserialize() {
        let directory: Content =
            serde_json::from_str(r#"{"type":"directory","info":{"name":"sub/","modified":null}}"#)
                .unwrap();
        assert!(matches!(
            directory,
            Content::Directory(DirectoryInfo {
                size: None,
                unix: None,
                ..
            })
        ));
        let file: Content = serde_json::from_str(
            r#"{"type":"file","info":{"name":"a","size":1024,"modified":null}}"#,
        )
        .unwrap();
        assert!(matches!(file, Content::File(FileInfo { unix: None, .. })));
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

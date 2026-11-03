/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! List buckets, a directory, or a file, with display filtering and sorting.
mod sort;

use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::{
    core::{SettingValue, Settings},
    daemon::Daemon,
    error::NonRetryableError,
    listing::{Content, DisplayOptions, ListingSink},
    transfer::{has_wildmat, wildmat},
};
use anyhow::{bail, Result};
use std::cmp::Ordering;

const USAGE: &str = "usage: ls [-FlarSh] [--] [path]";

pub(crate) fn register_settings(settings: &mut Settings) {
    settings.register(
        "ui:ls-default-options",
        SettingValue::Text("-lF".into()),
        "Base ls options; command-line flags override them (--no-* disables flags)",
    );
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct LsArgs {
    path: Option<String>,
    display: DisplayOptions,
    all: bool,
    size: bool,
    reverse: bool,
}
impl LsArgs {
    fn parse<'a>(words: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        Self::default().apply(words)
    }

    fn with_defaults<'a>(defaults: &str, words: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        let tokens = shlex::split(defaults)
            .ok_or_else(|| anyhow::anyhow!("invalid ui:ls-default-options: invalid quoting"))?;
        let args = Self::parse(tokens.iter().map(String::as_str))
            .and_then(|args| {
                if args.path.is_some() {
                    bail!("expected options only, not a path");
                }
                Ok(args)
            })
            .map_err(|error| anyhow::anyhow!("invalid ui:ls-default-options: {error}"))?;
        args.apply(words)
    }

    fn apply<'a>(mut self, words: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        let mut options = true;
        for word in words {
            if options && word == "--" {
                options = false;
            } else if options && word.starts_with("--") {
                match word {
                    "--no-classify" => self.display.classify = false,
                    "--no-long" => self.display.long = false,
                    "--no-human-readable" => self.display.human_readable = false,
                    "--no-all" => self.all = false,
                    "--no-size" => self.size = false,
                    "--no-reverse" => self.reverse = false,
                    _ => bail!("unknown ls option {word}; {USAGE}"),
                }
            } else if options && word.starts_with('-') && word != "-" {
                for flag in word[1..].chars() {
                    match flag {
                        'F' => self.display.classify = true,
                        'l' => self.display.long = true,
                        'h' => self.display.human_readable = true,
                        'a' => self.all = true,
                        'S' => self.size = true,
                        'r' => self.reverse = true,
                        _ => bail!("unknown ls option -{flag}; {USAGE}"),
                    }
                }
            } else if self.path.replace(word.to_owned()).is_some() {
                bail!("{USAGE}");
            }
        }
        Ok(self)
    }

    fn compare(&self, left: &Content, right: &Content) -> Ordering {
        let (left_name, left_directory, left_size) = attributes(left);
        let (right_name, right_directory, right_size) = attributes(right);
        let order = if self.size {
            // Unknown sizes sort after known sizes, without pretending they are zero.
            right_size
                .cmp(&left_size)
                .then_with(|| left_name.cmp(right_name))
                .then_with(|| right_directory.cmp(&left_directory))
        } else {
            right_directory
                .cmp(&left_directory)
                .then_with(|| left_name.cmp(right_name))
        };
        if self.reverse {
            order.reverse()
        } else {
            order
        }
    }
}

fn attributes(content: &Content) -> (&str, bool, Option<u64>) {
    match content {
        Content::Directory(info) => (info.name.trim_end_matches('/'), true, info.size),
        Content::Bucket(info) => (info.name.trim_end_matches('/'), true, None),
        Content::File(info) => (&info.name, false, info.size),
        Content::Comment(info) => (&info.text, false, None),
    }
}

pub(super) struct Ls;
impl Command for Ls {
    fn names(&self) -> &'static [&'static str] {
        &["ls"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "ls [-FlarSh] [path]",
            "list entries; base: ui:ls-default-options, --no-* disables",
        ))
    }
    fn completion(&self) -> Completion {
        Completion::RemotePath {
            kind: crate::ipc::pb::CompletionKind::RemotePath,
            multiple_arguments: false,
        }
    }
    fn accepts_path_completion(&self, arguments: &[String], path: &str) -> bool {
        LsArgs::parse(arguments.iter().map(String::as_str)).is_ok_and(|args| args.path.is_none())
            && (!path.starts_with('-') || arguments.iter().any(|arg| arg == "--"))
    }
}

fn execute_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> CommandResult {
    let args = match LsArgs::with_defaults(
        app.settings.text("ui:ls-default-options").unwrap_or("-lF"),
        parser.words(),
    ) {
        Ok(args) => args,
        Err(error) => return vec![error.to_string()].into(),
    };
    let connection = app.connection.clone();
    let snapshot = std::sync::Arc::new(std::sync::Mutex::new(None));
    let id = app
        .jobs
        .start_retrying_operation(parser.line.to_owned(), move |mut output| {
            let connection = connection.clone();
            let args = args.clone();
            let snapshot = snapshot.clone();
            async move {
                let mut listing = sort::SortedListing::new(args.clone());
                {
                    let active = connection.lock().await;
                    let active = active.as_ref().ok_or_else(|| {
                        crate::error::NonRetryableError("not connected; use connect first".into())
                    })?;
                    let path = args.path.as_deref().unwrap_or("");
                    if has_wildmat(path) {
                        let target = snapshot_listing_path(active.as_ref(), "", &snapshot)?;
                        let mut selected = WildmatListing {
                            pattern: path,
                            inner: &mut listing,
                            matches: 0,
                        };
                        active.list(&target, &mut selected).await?;
                        if selected.matches == 0 {
                            return Err(NonRetryableError(format!(
                                "no remote files match {path:?}"
                            ))
                            .into());
                        }
                    } else {
                        let target = snapshot_listing_path(active.as_ref(), path, &snapshot)?;
                        active.list(&target, &mut listing).await?;
                    }
                }
                listing.write_to(&mut output).await?;
                // Successful ls output contains items only, not a summary/footer.
                Ok(String::new())
            }
        });
    CommandResult::JobStarted(id)
}

struct WildmatListing<'a> {
    pattern: &'a str,
    inner: &'a mut dyn ListingSink,
    matches: usize,
}
impl ListingSink for WildmatListing<'_> {
    fn emit(&mut self, content: Content) -> Result<()> {
        let name = match &content {
            Content::File(file) => Some(file.name.as_str()),
            Content::Directory(directory) => Some(directory.name.trim_end_matches('/')),
            Content::Bucket(_) | Content::Comment(_) => None,
        };
        if name.is_some_and(|name| wildmat(self.pattern, name)) {
            self.matches += 1;
            self.inner.emit(content)?;
        }
        Ok(())
    }
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(execute_lines(context.daemon()?, parser))
}

pub(crate) fn snapshot_listing_path(
    connection: &dyn crate::backend::Connection,
    path: &str,
    snapshot: &std::sync::Mutex<Option<String>>,
) -> Result<String> {
    let mut saved = snapshot.lock().expect("listing snapshot poisoned");
    if saved.is_none() {
        *saved = Some(connection.resolve_listing_path(path)?);
    }
    Ok(saved.as_ref().unwrap().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listing::{BucketInfo, DirectoryInfo, FileInfo};

    pub(super) fn file(name: &str, size: Option<u64>) -> Content {
        Content::File(FileInfo {
            name: name.into(),
            size,
            modified: None,
            unix: None,
        })
    }
    pub(super) fn directory(name: &str, size: Option<u64>) -> Content {
        Content::Directory(DirectoryInfo {
            name: name.into(),
            size,
            modified: None,
            unix: None,
        })
    }
    pub(super) fn entries() -> Vec<Content> {
        vec![
            file("zeta", Some(50)),
            directory("zoo/", None),
            file(".hidden", Some(500)),
            file("alpha", Some(10)),
            file("unknown", None),
            directory("aardvark/", Some(4)),
            Content::Bucket(BucketInfo {
                name: "archive".into(),
                created: None,
            }),
            file("beta", Some(50)),
            file("zero", Some(0)),
        ]
    }

    #[test]
    fn wildcard_listing_keeps_matching_files_and_directories() {
        struct Collected(Vec<Content>);
        impl ListingSink for Collected {
            fn emit(&mut self, content: Content) -> Result<()> {
                self.0.push(content);
                Ok(())
            }
        }
        let mut collected = Collected(Vec::new());
        let mut selected = WildmatListing {
            pattern: "*.txt",
            inner: &mut collected,
            matches: 0,
        };
        selected.emit(file("one.txt", None)).unwrap();
        selected.emit(directory("folder.txt", None)).unwrap();
        selected.emit(file("two.md", None)).unwrap();
        assert_eq!(selected.matches, 2);
        assert_eq!(attributes(&collected.0[0]).0, "one.txt");
        assert_eq!(attributes(&collected.0[1]).0, "folder.txt");
    }

    #[tokio::test(start_paused = true)]
    async fn retries_keep_options_and_the_original_resolved_path_without_partial_output() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
            Arc, Mutex,
        };
        struct RetryListing {
            attempts: Arc<AtomicUsize>,
            cwd: Arc<Mutex<String>>,
            paths: Arc<Mutex<Vec<String>>>,
        }
        #[async_trait::async_trait]
        impl crate::backend::Connection for RetryListing {
            fn protocol(&self) -> &'static str {
                "test"
            }
            fn resolve_listing_path(&self, path: &str) -> Result<String> {
                Ok(format!("{}/{path}", self.cwd.lock().unwrap()))
            }
            async fn list(
                &self,
                path: &str,
                output: &mut dyn crate::listing::ListingSink,
            ) -> Result<u64> {
                self.paths.lock().unwrap().push(path.into());
                for entry in [
                    file("z", Some(2)),
                    directory("b/", None),
                    file(".hidden", Some(0)),
                    file("a", Some(1)),
                ] {
                    output.emit(entry)?;
                }
                if self.attempts.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                    bail!("transport reset");
                }
                Ok(4)
            }
        }
        let attempts = Arc::new(AtomicUsize::new(0));
        let cwd = Arc::new(Mutex::new("/original".into()));
        let paths = Arc::new(Mutex::new(Vec::new()));
        let mut app = Daemon::bootstrap().unwrap();
        *app.connection.lock().await = Some(Box::new(RetryListing {
            attempts: attempts.clone(),
            cwd: cwd.clone(),
            paths: paths.clone(),
        }));
        app.command("set ui:ls-default-options '-laSrF'");
        let response = app.command("ls --no-long prefix");
        let id: u64 = response[0].strip_prefix("JOB ").unwrap().parse().unwrap();
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 1);
        assert!(app.jobs.jobs[&id].output.read_all().is_empty());
        *cwd.lock().unwrap() = "/changed".into();
        app.command("set ui:ls-default-options ''");
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if !app.has_running_jobs() {
                break;
            }
        }
        assert_eq!(app.jobs.jobs[&id].state, crate::core::JobState::Completed);
        assert_eq!(
            *paths.lock().unwrap(),
            vec!["/original/prefix", "/original/prefix"]
        );
        assert_eq!(
            app.jobs.jobs[&id].output.read_all(),
            vec!["b/", ".hidden", "a", "z"]
        );
        std::fs::remove_file(app.jobs.jobs[&id].output.path()).unwrap();
    }

    #[test]
    fn options_support_combined_flags_separate_flags_paths_and_end_of_options() {
        let combined = LsArgs::parse(["-FlarS", "photo album"]).unwrap();
        assert_eq!(
            combined,
            LsArgs::parse(["-F", "-l", "-a", "photo album", "-r", "-S"]).unwrap()
        );
        assert_eq!(combined.path.as_deref(), Some("photo album"));
        assert!(
            combined.display.long
                && combined.display.classify
                && combined.all
                && combined.size
                && combined.reverse
        );
        assert_eq!(
            LsArgs::parse(["--", "-literal"]).unwrap().path.as_deref(),
            Some("-literal")
        );
        assert_eq!(LsArgs::parse(["-"]).unwrap().path.as_deref(), Some("-"));
        assert_eq!(LsArgs::parse([]).unwrap(), LsArgs::default());
        assert_eq!(
            LsArgs::parse(["-aa", "-FF"]).unwrap(),
            LsArgs::parse(["-aF"]).unwrap()
        );
        assert!(LsArgs::parse(["-z"])
            .unwrap_err()
            .to_string()
            .contains("unknown ls option -z"));
        assert_eq!(LsArgs::parse(["a", "b"]).unwrap_err().to_string(), USAGE);
        assert!(LsArgs::parse(["--", "-a", "b"]).is_err());
    }

    #[test]
    fn human_readable_flag_combines_and_can_override_defaults() {
        let args = LsArgs::parse(["-lhS"]).unwrap();
        assert!(args.display.long && args.display.human_readable && args.size);
        assert_eq!(args, LsArgs::parse(["-l", "-h", "-S"]).unwrap());
        assert!(
            !LsArgs::with_defaults("-lh", ["--no-human-readable"])
                .unwrap()
                .display
                .human_readable
        );
        assert!(
            LsArgs::with_defaults("-lh", ["--no-human-readable", "-h"])
                .unwrap()
                .display
                .human_readable
        );
        assert_eq!(
            args.compare(&file("small", Some(1024)), &file("large", Some(2048))),
            Ordering::Greater
        );
    }

    #[test]
    fn default_options_are_applied_first_and_cli_flags_override_individual_values() {
        assert_eq!(
            LsArgs::with_defaults("-lF", []).unwrap(),
            LsArgs::parse(["-lF"]).unwrap()
        );
        assert_eq!(
            LsArgs::with_defaults("-lF", ["-S", "photos"]).unwrap(),
            LsArgs::parse(["-lFS", "photos"]).unwrap()
        );
        let flags = [
            "--no-long",
            "--no-classify",
            "--no-all",
            "--no-size",
            "--no-reverse",
        ];
        assert_eq!(
            LsArgs::with_defaults("-laFSr", flags).unwrap(),
            LsArgs::default()
        );
        assert_eq!(
            LsArgs::with_defaults("-laFSr", ["--no-size", "--no-reverse", "-Sr"]).unwrap(),
            LsArgs::parse(["-laFSr"]).unwrap()
        );
        assert_eq!(
            LsArgs::with_defaults("--no-long --no-classify", ["-lF"]).unwrap(),
            LsArgs::parse(["-lF"]).unwrap()
        );
        assert_eq!(
            LsArgs::with_defaults("", ["-a", "--", "-literal"]).unwrap(),
            LsArgs::parse(["-a", "--", "-literal"]).unwrap()
        );
        assert_eq!(
            LsArgs::with_defaults("-l -F", ["--", "-a"])
                .unwrap()
                .path
                .as_deref(),
            Some("-a")
        );
    }

    #[test]
    fn invalid_default_options_fail_before_starting_a_job() {
        let mut app = Daemon::bootstrap().unwrap();
        for value in ["-z", "photos", "-l photos", "'unfinished"] {
            app.settings.set("ui:ls-default-options", value).unwrap();
            assert!(
                app.command("ls")[0].starts_with("invalid ui:ls-default-options:"),
                "{value}"
            );
            assert!(app.jobs.jobs.is_empty());
        }
    }

    #[test]
    fn default_options_setting_can_be_loaded_shown_updated_and_cleared() {
        let mut settings = Settings::new();
        register_settings(&mut settings);
        assert_eq!(settings.text("ui:ls-default-options"), Some("-lF"));
        assert!(settings.text("ls-default-options").is_none());
        assert!(settings.set("ls-default-options", "-a").is_err());
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("xfer.conf");
        std::fs::write(&config, "ui:ls-default-options: -aS\n").unwrap();
        settings.load_file(&config).unwrap();
        assert_eq!(settings.text("ui:ls-default-options"), Some("-aS"));
        let mut app = Daemon::bootstrap().unwrap();
        assert!(app.command("set ui:ls-default-options '-aS'")[0].contains("set to -aS"));
        assert!(
            app.command("show ui:ls-default-options")[0].starts_with("ui:ls-default-options: -aS")
        );
        app.command("set ui:ls-default-options ''");
        assert_eq!(app.settings.text("ui:ls-default-options"), Some(""));
        assert_eq!(
            LsArgs::with_defaults(app.settings.text("ui:ls-default-options").unwrap(), []).unwrap(),
            LsArgs::default()
        );
    }

    #[test]
    fn sorting_groups_by_directory_or_orders_by_size_and_reverses_the_whole_order() {
        for (flags, expected) in [
            (
                "-a",
                vec![
                    "aardvark", "archive", "zoo", ".hidden", "alpha", "beta", "unknown", "zero",
                    "zeta",
                ],
            ),
            (
                "-aS",
                vec![
                    ".hidden", "beta", "zeta", "alpha", "aardvark", "zero", "archive", "unknown",
                    "zoo",
                ],
            ),
        ] {
            let mut args = LsArgs::parse([flags]).unwrap();
            let mut records = entries();
            records.sort_unstable_by(|a, b| args.compare(a, b));
            assert_eq!(
                records
                    .iter()
                    .map(|entry| attributes(entry).0)
                    .collect::<Vec<_>>(),
                expected
            );
            args.reverse = true;
            records.sort_unstable_by(|a, b| args.compare(a, b));
            assert_eq!(
                records
                    .iter()
                    .map(|entry| attributes(entry).0)
                    .collect::<Vec<_>>(),
                expected.into_iter().rev().collect::<Vec<_>>()
            );
        }
    }
}

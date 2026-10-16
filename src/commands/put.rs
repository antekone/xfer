//! Upload local files to the current remote directory.
use super::{Command, CommandContext, CommandResult, Handler, ParserContext};
use crate::{
    error::NonRetryableError,
    transfer::{backoff, has_wildmat, safe_name, wildmat, TransferProgress},
};
use anyhow::{bail, Result};
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

pub(super) struct Put;
impl Command for Put {
    fn names(&self) -> &'static [&'static str] {
        &["put"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "put [--] <files...>",
            "upload local files to the remote directory",
        ))
    }
}
fn args(words: impl Iterator<Item = impl AsRef<str>>) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut options = true;
    for word in words {
        let word = word.as_ref();
        match word {
            "--" if options => options = false,
            flag if options && flag.starts_with('-') => {
                bail!("unknown put option {flag}; usage: put [--] <files...>")
            }
            file => files.push(file.to_owned()),
        }
    }
    if files.is_empty() {
        bail!("usage: put [--] <files...>");
    }
    Ok(files)
}
#[derive(Debug)]
struct Plan {
    local: PathBuf,
    remote_name: String,
    size: u64,
}
fn expand_local(files: Vec<String>, directory: &Path) -> Result<Vec<String>> {
    if !files.iter().any(|file| has_wildmat(file)) {
        return Ok(files);
    }
    let mut available = fs::read_dir(directory)
        .map_err(|error| {
            NonRetryableError(format!("local directory {}: {error}", directory.display()))
        })?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            entry.file_type().ok().filter(|kind| kind.is_file())?;
            entry.file_name().into_string().ok()
        })
        .collect::<Vec<_>>();
    available.sort();
    let mut expanded = Vec::new();
    for input in files {
        if !has_wildmat(&input) {
            expanded.push(input);
            continue;
        }
        let matches = available
            .iter()
            .filter(|name| wildmat(&input, name))
            .cloned()
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(NonRetryableError(format!("no local files match {input:?}")).into());
        }
        expanded.extend(matches);
    }
    Ok(expanded)
}

fn plans(files: Vec<String>, directory: &Path) -> Result<Vec<Plan>> {
    let mut names = HashSet::new();
    let mut plans = Vec::new();
    for input in files {
        let local = directory.join(&input);
        let metadata = fs::symlink_metadata(&local)
            .map_err(|e| NonRetryableError(format!("local file {}: {e}", local.display())))?;
        if !metadata.file_type().is_file() {
            bail!("put requires a regular local file: {}", local.display());
        }
        let name = Path::new(&input)
            .file_name()
            .and_then(|n| n.to_str())
            .filter(|n| !n.is_empty())
            .ok_or_else(|| NonRetryableError("put requires a filename".into()))?
            .to_owned();
        if !names.insert(name.clone()) {
            bail!("multiple sources would upload to the same remote filename: {name}");
        }
        plans.push(Plan {
            local,
            remote_name: name,
            size: metadata.len(),
        });
    }
    Ok(plans)
}
fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let files = args(parser.words())?;
    let app = context.daemon()?;
    let connection = app.connection.clone();
    let directory = app.local_directory.clone();
    let clobber = app.settings.bool("xfer:clobber");
    let id = app.jobs.start_task(
        parser.line.to_owned(),
        move |mut output, progress| async move {
            let plans = plans(expand_local(files, &directory)?, &directory)?;
            let active = connection.lock().await;
            let active = active
                .as_ref()
                .ok_or_else(|| NonRetryableError("not connected; use connect first".into()))?;
            let total = plans.iter().map(|p| p.size).sum();
            let started = std::time::Instant::now();
            let count = plans.len();
            let mut done = 0;
            for (index, plan) in plans.iter().enumerate() {
                let remote = active.resolve_listing_path(&plan.remote_name)?;
                let mut delay = Duration::from_secs(3);
                let mut attempt = 1;
                loop {
                    let _ = progress.send(crate::core::Progress::Transfer(TransferProgress {
                        file: plan.remote_name.clone(),
                        file_index: index + 1,
                        file_count: count,
                        done,
                        total,
                        transferred: done,
                        elapsed_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                    }));
                    match active.upload(&plan.local, &remote, clobber).await {
                        Ok(()) => break,
                        Err(error) => {
                            backoff(
                                error,
                                &safe_name(&plan.remote_name),
                                attempt,
                                delay,
                                &progress,
                            )
                            .await?;
                            delay = (delay * 2).min(Duration::from_secs(30));
                            attempt += 1;
                        }
                    }
                }
                done += plan.size;
                output.write_line(&format!(
                    "uploaded {} ({} bytes)",
                    safe_name(&plan.remote_name),
                    plan.size
                ))?;
            }
            Ok(format!(
                "uploaded {count} file(s) in {:.1}s",
                started.elapsed().as_secs_f64()
            ))
        },
    );
    Ok(vec![format!("JOB {id}")].into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_support_end_of_options() {
        assert_eq!(args(["--", "-report"].iter()).unwrap(), ["-report"]);
        assert!(args(["-x"].iter())
            .unwrap_err()
            .to_string()
            .contains("unknown put option"));
        assert!(args(std::iter::empty::<&str>()).is_err());
    }

    #[test]
    fn plans_require_regular_unique_local_files() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("one"), b"abc").unwrap();
        let upload_plans = plans(vec!["one".into()], directory.path()).unwrap();
        assert_eq!(upload_plans[0].remote_name, "one");
        assert_eq!(upload_plans[0].size, 3);
        fs::create_dir(directory.path().join("folder")).unwrap();
        fs::write(directory.path().join("two.txt"), b"x").unwrap();
        assert_eq!(
            expand_local(vec!["*.txt".into()], directory.path()).unwrap(),
            ["two.txt"]
        );
        assert!(plans(vec!["one".into(), "./one".into()], directory.path())
            .unwrap_err()
            .to_string()
            .contains("same remote filename"));
    }
}

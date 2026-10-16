//! Local path resolution and filesystem-only directory/file completion.
use crate::cache::PathCompletion;
use anyhow::{bail, Result};
use std::{
    fs,
    path::{Path, PathBuf},
};

const MAX_CANDIDATES: usize = 2048;
const MAX_BYTES: usize = 1024 * 1024;

fn home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}
fn resolve_with_home(base: &Path, input: &str, home: Option<&Path>) -> Result<PathBuf> {
    let path = if input == "~" || input.starts_with("~/") {
        let home = home.ok_or_else(|| {
            anyhow::anyhow!("cannot resolve local home directory: HOME is not set")
        })?;
        home.join(
            input
                .strip_prefix("~/")
                .unwrap_or("")
                .trim_start_matches('/'),
        )
    } else {
        PathBuf::from(input)
    };
    Ok(base.join(path))
}
pub(crate) fn resolve(base: &Path, input: &str) -> Result<PathBuf> {
    resolve_with_home(base, input, home().as_deref())
}
pub(crate) fn complete_directories(base: &Path, input: &str) -> Result<PathCompletion> {
    complete_with_home(base, input, home().as_deref())
}
pub(crate) fn complete_files(base: &Path, input: &str) -> Result<PathCompletion> {
    complete(base, input, home().as_deref(), Kind::Files)
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Directories,
    Files,
}
fn complete_with_home(base: &Path, input: &str, home: Option<&Path>) -> Result<PathCompletion> {
    complete(base, input, home, Kind::Directories)
}
fn complete(base: &Path, input: &str, home: Option<&Path>, kind: Kind) -> Result<PathCompletion> {
    if kind == Kind::Directories && input == "~" {
        let directory = resolve_with_home(base, input, home)?;
        if !directory.is_dir() {
            bail!("local home directory is not an accessible directory: {directory:?}");
        }
        return Ok(PathCompletion {
            candidates: vec!["~/".into()],
            truncated: false,
        });
    }
    let (parent, prefix) = match input.rfind('/') {
        Some(index) => (&input[..=index], &input[index + 1..]),
        None => ("", input),
    };
    let directory = resolve_with_home(base, parent, home)?;
    let error = |error| anyhow::anyhow!("cannot read local directory {directory:?}: {error}");
    let entries = fs::read_dir(&directory).map_err(error)?;
    let mut candidates = Vec::new();
    let mut bytes = 0;
    let mut truncated = false;
    // Match shell-style dot navigation without spoiling unique completion for
    // an empty prefix. Preserve the user's path spelling, including ~/ and ../.
    for name in ["./", "../"] {
        if kind == Kind::Directories && prefix.starts_with('.') && name.starts_with(prefix) {
            let candidate = format!("{parent}{name}");
            bytes += candidate.len();
            candidates.push(candidate);
        }
    }
    for entry in entries {
        let entry = entry.map_err(error)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(prefix)
            || (name.starts_with('.') && !prefix.starts_with('.'))
            || name.chars().any(char::is_control)
        {
            continue;
        }
        let file_type = entry.file_type().map_err(error)?;
        let candidate = match kind {
            Kind::Directories
                if file_type.is_dir() || (file_type.is_symlink() && entry.path().is_dir()) =>
            {
                format!("{parent}{name}/")
            }
            // put rejects final symlinks and special files during preflight too.
            Kind::Files if file_type.is_file() => {
                if parent.is_empty() && prefix.is_empty() && (name.starts_with('-') || name == "~")
                {
                    format!("./{name}")
                } else {
                    format!("{parent}{name}")
                }
            }
            _ => continue,
        };
        if candidates.len() >= MAX_CANDIDATES || candidate.len() > MAX_BYTES.saturating_sub(bytes) {
            truncated = true;
            break;
        }
        bytes += candidate.len();
        candidates.push(candidate);
    }
    candidates.sort_unstable();
    Ok(PathCompletion {
        candidates,
        truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_completion_excludes_directories_symlinks_and_special_files() {
        let root = tempfile::tempdir().unwrap();
        for name in [
            "z.txt",
            "photo album 日本語.txt",
            ".hidden",
            "-literal",
            "~",
            "unsafe\nname",
        ] {
            fs::write(root.path().join(name), b"data").unwrap();
        }
        fs::create_dir(root.path().join("photo directory")).unwrap();
        std::os::unix::fs::symlink("z.txt", root.path().join("linked file")).unwrap();
        std::os::unix::fs::symlink("photo directory", root.path().join("linked directory"))
            .unwrap();
        let _socket = std::os::unix::net::UnixListener::bind(root.path().join("socket")).unwrap();
        assert_eq!(
            complete_files(root.path(), "").unwrap().candidates,
            vec!["./-literal", "./~", "photo album 日本語.txt", "z.txt"]
        );
        assert_eq!(
            complete_files(root.path(), "pho").unwrap().candidates,
            vec!["photo album 日本語.txt"]
        );
        assert_eq!(
            complete_files(root.path(), ".").unwrap().candidates,
            vec![".hidden"]
        );
        assert_eq!(
            complete_files(root.path(), "-li").unwrap().candidates,
            vec!["-literal"]
        );
    }

    #[test]
    fn file_completion_preserves_nested_absolute_and_home_paths_and_is_fresh() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        fs::create_dir_all(home.join("Downloads")).unwrap();
        let file = home.join("Downloads/report.txt");
        fs::write(&file, b"data").unwrap();
        assert_eq!(
            complete(root.path(), "~/Downloads/rep", Some(&home), Kind::Files)
                .unwrap()
                .candidates,
            vec!["~/Downloads/report.txt"]
        );
        assert_eq!(
            complete_files(&home, "Downloads/rep").unwrap().candidates,
            vec!["Downloads/report.txt"]
        );
        assert_eq!(
            complete_files(&home.join("Downloads"), "../Downloads/rep")
                .unwrap()
                .candidates,
            vec!["../Downloads/report.txt"]
        );
        assert_eq!(
            complete_files(root.path(), file.to_str().unwrap())
                .unwrap()
                .candidates,
            vec![file.to_str().unwrap()]
        );
        fs::remove_file(file).unwrap();
        assert!(
            complete(root.path(), "~/Downloads/rep", Some(&home), Kind::Files)
                .unwrap()
                .candidates
                .is_empty()
        );
    }

    #[test]
    fn completion_is_directory_only_sorted_and_follows_only_directory_symlinks() {
        let root = tempfile::tempdir().unwrap();
        for name in ["zebra", "photo album", "photos", ".private", "日本語"] {
            fs::create_dir(root.path().join(name)).unwrap();
        }
        fs::write(root.path().join("photo.txt"), b"file").unwrap();
        std::os::unix::fs::symlink("photos", root.path().join("linked photos")).unwrap();
        std::os::unix::fs::symlink("photo.txt", root.path().join("linked file")).unwrap();
        std::os::unix::fs::symlink("missing", root.path().join("broken")).unwrap();
        let result = complete_with_home(root.path(), "", None).unwrap();
        assert_eq!(
            result.candidates,
            vec![
                "linked photos/",
                "photo album/",
                "photos/",
                "zebra/",
                "日本語/"
            ]
        );
        assert!(!result.truncated);
        assert_eq!(
            complete_with_home(root.path(), "pho", None)
                .unwrap()
                .candidates,
            vec!["photo album/", "photos/"]
        );
        assert_eq!(
            complete_with_home(root.path(), ".", None)
                .unwrap()
                .candidates,
            vec!["../", "./", ".private/"]
        );
        assert_eq!(
            complete_with_home(root.path(), "..", None)
                .unwrap()
                .candidates,
            vec!["../"]
        );
    }

    #[test]
    fn relative_absolute_and_home_prefixes_keep_the_users_path_spelling() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        fs::create_dir_all(home.join("Downloads/nested")).unwrap();
        assert_eq!(
            complete_with_home(root.path(), "~", Some(&home))
                .unwrap()
                .candidates,
            vec!["~/"]
        );
        assert_eq!(
            complete_with_home(root.path(), "~/Do", Some(&home))
                .unwrap()
                .candidates,
            vec!["~/Downloads/"]
        );
        assert_eq!(
            complete_with_home(root.path(), "~/Downloads/ne", Some(&home))
                .unwrap()
                .candidates,
            vec!["~/Downloads/nested/"]
        );
        assert_eq!(
            complete_with_home(&home.join("Downloads"), "../Do", None)
                .unwrap()
                .candidates,
            vec!["../Downloads/"]
        );
        let absolute = format!("{}/home/Do", root.path().display());
        assert_eq!(
            complete_with_home(Path::new("/"), &absolute, None)
                .unwrap()
                .candidates,
            vec![format!("{}/home/Downloads/", root.path().display())]
        );
        assert_eq!(
            resolve_with_home(root.path(), "~/Downloads", Some(&home)).unwrap(),
            home.join("Downloads")
        );
        assert!(complete_with_home(root.path(), "~/Do", None).is_err());
        assert!(resolve_with_home(root.path(), "~", None).is_err());
    }

    #[test]
    fn completion_is_fresh_and_missing_or_non_directory_parents_report_errors() {
        let root = tempfile::tempdir().unwrap();
        assert!(complete_with_home(root.path(), "new", None)
            .unwrap()
            .candidates
            .is_empty());
        fs::create_dir(root.path().join("new")).unwrap();
        assert_eq!(
            complete_with_home(root.path(), "new", None)
                .unwrap()
                .candidates,
            vec!["new/"]
        );
        fs::remove_dir(root.path().join("new")).unwrap();
        assert!(complete_with_home(root.path(), "new", None)
            .unwrap()
            .candidates
            .is_empty());
        fs::write(root.path().join("file"), b"not a directory").unwrap();
        assert!(complete_with_home(root.path(), "file/", None).is_err());
        assert!(complete_with_home(root.path(), "missing/", None).is_err());
    }

    #[test]
    fn non_utf8_names_and_terminal_control_characters_are_not_candidates() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};
        let root = tempfile::tempdir().unwrap();
        for name in [
            OsString::from("safe"),
            OsString::from("unsafe\nname"),
            OsString::from_vec(vec![255]),
        ] {
            fs::create_dir(root.path().join(name)).unwrap();
        }
        assert_eq!(
            complete_with_home(root.path(), "", None)
                .unwrap()
                .candidates,
            vec!["safe/"]
        );
    }

    #[test]
    fn huge_matching_sets_are_bounded_and_marked_incomplete() {
        let root = tempfile::tempdir().unwrap();
        for index in 0..=MAX_CANDIDATES {
            fs::create_dir(root.path().join(format!("dir-{index:04}"))).unwrap();
        }
        let result = complete_with_home(root.path(), "dir-", None).unwrap();
        assert!(result.truncated);
        assert_eq!(result.candidates.len(), MAX_CANDIDATES);
        assert!(result.candidates.windows(2).all(|pair| pair[0] < pair[1]));
    }
}

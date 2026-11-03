/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Per-user application paths with native Linux, macOS and Windows defaults.
//!
//! Explicit absolute XDG overrides are accepted on every platform (also useful
//! for sandboxing). Missing, empty or relative overrides use `directories`' OS
//! lookup instead. Never fall back to the process working directory.
use directories::{BaseDirs, ProjectDirs};
use std::{env, fs, io, path::PathBuf};

fn xdg_override(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}

fn project_dirs() -> ProjectDirs {
    // On Unix, the crate falls back to the account database when HOME is absent.
    // Windows uses Known Folders rather than depending on HOME/APPDATA variables.
    ProjectDirs::from("", "", "xfer")
        .expect("cannot determine application directories for the current OS user")
}

/// Native home directory, including the Unix account-database fallback.
pub fn home_dir() -> Option<PathBuf> {
    BaseDirs::new().map(|dirs| dirs.home_dir().to_path_buf())
}

/// Directory shared by xfer.conf and bookmarks.toml.
pub fn config_dir() -> PathBuf {
    xdg_override("XDG_CONFIG_HOME")
        .map(|base| base.join("xfer"))
        .unwrap_or_else(|| project_dirs().config_dir().to_path_buf())
}

/// Disposable application data, including job output spools.
pub fn cache_dir() -> PathBuf {
    xdg_override("XDG_CACHE_HOME")
        .map(|base| base.join("xfer"))
        .unwrap_or_else(|| project_dirs().cache_dir().to_path_buf())
}

/// Persistent local state, including command history. macOS and Windows have no
/// distinct state directory, so use the crate's local application-data folder.
pub fn state_dir() -> PathBuf {
    xdg_override("XDG_STATE_HOME")
        .map(|base| base.join("xfer"))
        .unwrap_or_else(|| {
            let dirs = project_dirs();
            dirs.state_dir()
                .unwrap_or_else(|| dirs.data_local_dir())
                .to_path_buf()
        })
}

/// Private daemon socket/discovery directory. Prefer the OS runtime directory;
/// when none exists, use persistent local state (never a shared temp directory).
fn runtime_path() -> PathBuf {
    xdg_override("XDG_RUNTIME_DIR")
        .map(|base| base.join("xfer"))
        .or_else(|| project_dirs().runtime_dir().map(PathBuf::from))
        .unwrap_or_else(state_dir)
        .join("instances")
}

/// Create and return the instance directory, using private Unix permissions
/// for newly created directories and native inherited permissions elsewhere.
pub fn runtime_dir() -> io::Result<PathBuf> {
    let path = runtime_path();
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&path)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::{ffi::OsStr, path::Path, process::Command};

    const PROBE: &str = "XFER_TEST_DIRECTORY_PROBE";
    const PREFIX: &str = "XFER_DIRECTORY_SNAPSHOT=";
    const VARIABLES: &[&str] = &[
        "XDG_CONFIG_HOME",
        "XDG_CACHE_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
        "XDG_RUNTIME_DIR",
        "XFER_CONFIG",
    ];

    #[derive(Serialize, Deserialize)]
    struct Snapshot {
        home: Option<PathBuf>,
        config: PathBuf,
        cache: PathBuf,
        state: PathBuf,
        runtime: PathBuf,
        bookmarks: PathBuf,
        native_config: PathBuf,
        native_cache: PathBuf,
        native_state: PathBuf,
    }

    // Isolate environment changes in child processes instead of racing the
    // rest of the test suite's daemon/configuration setup.
    #[test]
    fn directory_probe() {
        if env::var_os(PROBE).is_none() {
            return;
        }
        let native = project_dirs();
        let snapshot = Snapshot {
            home: home_dir(),
            config: config_dir(),
            cache: cache_dir(),
            state: state_dir(),
            runtime: runtime_path(),
            bookmarks: crate::bookmarks::default_path(),
            native_config: native.config_dir().into(),
            native_cache: native.cache_dir().into(),
            native_state: native
                .state_dir()
                .unwrap_or_else(|| native.data_local_dir())
                .into(),
        };
        if env::var_os("XFER_TEST_DIRECTORY_CREATE").is_some() {
            assert_eq!(runtime_dir().unwrap(), snapshot.runtime);
            assert!(snapshot.runtime.is_dir());
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(&snapshot.runtime)
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o700
                );
            }
            let mut spool = crate::core::OutputStore::new(1);
            assert!(spool.path().starts_with(snapshot.cache.join("jobs")));
            spool.write_line("directory test").unwrap();
            assert_eq!(spool.read_all(), ["directory test"]);
            fs::remove_file(spool.path()).unwrap();
            let daemon = crate::daemon::Daemon::bootstrap().unwrap();
            assert_eq!(
                daemon.settings.text("ui:history-file"),
                snapshot.state.join("history").to_str()
            );
            assert_eq!(
                daemon.settings.text("bookmarks:file"),
                snapshot.bookmarks.to_str()
            );
        }
        println!("{PREFIX}{}", serde_json::to_string(&snapshot).unwrap());
    }

    fn probe(home: Option<&Path>, overrides: &[(&str, &OsStr)], create: bool) -> Snapshot {
        let mut command = Command::new(env::current_exe().unwrap());
        command.args(["--exact", "paths::tests::directory_probe", "--nocapture"]);
        for variable in VARIABLES {
            command.env_remove(variable);
        }
        command.env_remove("HOME");
        if let Some(home) = home {
            command.env("HOME", home);
        }
        for (name, value) in overrides {
            command.env(name, value);
        }
        command.env(PROBE, "1");
        command.env_remove("XFER_TEST_DIRECTORY_CREATE");
        if create {
            command.env("XFER_TEST_DIRECTORY_CREATE", "1");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "child directory probe failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        serde_json::from_str(
            stdout
                .lines()
                .find_map(|line| line.strip_prefix(PREFIX))
                .expect("missing directory snapshot"),
        )
        .unwrap()
    }

    #[test]
    fn absolute_xdg_overrides_route_all_application_files_into_the_sandbox() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let paths = [
            root.join("config"),
            root.join("cache"),
            root.join("state"),
            root.join("runtime"),
        ];
        let snapshot = probe(
            Some(&root),
            &[
                ("XDG_CONFIG_HOME", paths[0].as_os_str()),
                ("XDG_CACHE_HOME", paths[1].as_os_str()),
                ("XDG_STATE_HOME", paths[2].as_os_str()),
                ("XDG_RUNTIME_DIR", paths[3].as_os_str()),
            ],
            true,
        );
        assert_eq!(snapshot.config, paths[0].join("xfer"));
        assert_eq!(snapshot.cache, paths[1].join("xfer"));
        assert_eq!(snapshot.state, paths[2].join("xfer"));
        assert_eq!(snapshot.runtime, paths[3].join("xfer/instances"));
        assert_eq!(snapshot.bookmarks, snapshot.config.join("bookmarks.toml"));
    }

    #[test]
    fn missing_xdg_variables_use_native_directories_not_the_working_directory() {
        let root = tempfile::tempdir().unwrap();
        let snapshot = probe(Some(root.path()), &[], false);
        assert_eq!(snapshot.config, snapshot.native_config);
        assert_eq!(snapshot.cache, snapshot.native_cache);
        assert_eq!(snapshot.state, snapshot.native_state);
        assert_eq!(snapshot.runtime, snapshot.state.join("instances"));
        assert!(
            snapshot.config.is_absolute()
                && snapshot.cache.is_absolute()
                && snapshot.state.is_absolute()
        );
        #[cfg(target_os = "linux")]
        {
            assert_eq!(snapshot.config, root.path().join(".config/xfer"));
            assert_eq!(snapshot.cache, root.path().join(".cache/xfer"));
            assert_eq!(snapshot.state, root.path().join(".local/state/xfer"));
        }
        #[cfg(target_os = "macos")]
        {
            assert_eq!(
                snapshot.config,
                root.path().join("Library/Application Support/xfer")
            );
            assert_eq!(snapshot.cache, root.path().join("Library/Caches/xfer"));
        }
    }

    #[test]
    fn empty_and_relative_xdg_variables_are_ignored() {
        let root = tempfile::tempdir().unwrap();
        for value in ["", "relative/path"] {
            let snapshot = probe(
                Some(root.path()),
                &[
                    ("XDG_CONFIG_HOME", OsStr::new(value)),
                    ("XDG_CACHE_HOME", OsStr::new(value)),
                    ("XDG_STATE_HOME", OsStr::new(value)),
                    ("XDG_RUNTIME_DIR", OsStr::new(value)),
                ],
                false,
            );
            assert_eq!(snapshot.config, snapshot.native_config);
            assert_eq!(snapshot.cache, snapshot.native_cache);
            assert_eq!(snapshot.state, snapshot.native_state);
            assert_eq!(snapshot.runtime, snapshot.state.join("instances"));
        }
    }

    #[test]
    fn missing_runtime_uses_state_override_without_changing_config_or_cache() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("state");
        let snapshot = probe(
            Some(root.path()),
            &[("XDG_STATE_HOME", state.as_os_str())],
            false,
        );
        assert_eq!(snapshot.runtime, state.join("xfer/instances"));
        assert_eq!(snapshot.config, snapshot.native_config);
        assert_eq!(snapshot.cache, snapshot.native_cache);
    }

    #[test]
    fn runtime_fallback_creates_private_directories_under_state() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("config");
        let cache = root.path().join("cache");
        let state = root.path().join("state");
        let snapshot = probe(
            Some(root.path()),
            &[
                ("XDG_CONFIG_HOME", config.as_os_str()),
                ("XDG_CACHE_HOME", cache.as_os_str()),
                ("XDG_STATE_HOME", state.as_os_str()),
            ],
            true,
        );
        assert_eq!(snapshot.runtime, state.join("xfer/instances"));
        assert!(snapshot.runtime.is_dir());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn missing_or_empty_home_uses_the_os_account_database() {
        for overrides in [vec![], vec![("HOME", OsStr::new(""))]] {
            let snapshot = probe(None, &overrides, false);
            let home = snapshot.home.expect("OS account has a home directory");
            assert!(home.is_absolute());
            assert_eq!(snapshot.config, home.join(".config/xfer"));
            assert_eq!(snapshot.cache, home.join(".cache/xfer"));
            assert_eq!(snapshot.state, home.join(".local/state/xfer"));
        }
    }
}

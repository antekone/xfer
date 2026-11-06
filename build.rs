use std::{
    env,
    path::Path,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty() && !value.chars().any(char::is_control)).then_some(value)
}

fn watch_git_path(path: &str) {
    if let Some(path) = git_output(&["rev-parse", "--git-path", path]) {
        if Path::new(&path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}

fn build_metadata() {
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    let timestamp = match env::var("SOURCE_DATE_EPOCH") {
        Ok(value) => value.parse::<i64>().ok(),
        Err(env::VarError::NotPresent) => SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_secs()).ok()),
        Err(_) => None,
    };
    let date = timestamp
        .and_then(|value| chrono::DateTime::from_timestamp(value, 0))
        .map(|date| date.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=XFER_BUILD_DATE={date}");

    // Do not accidentally identify a parent repository when building an archive.
    let commit = if Path::new(".git").exists() {
        watch_git_path("HEAD");
        watch_git_path("packed-refs");
        if let Some(reference) = git_output(&["symbolic-ref", "-q", "HEAD"]) {
            watch_git_path(&reference);
        }
        git_output(&["rev-parse", "--verify", "HEAD"]).filter(|value| {
            matches!(value.len(), 40 | 64) && value.bytes().all(|b| b.is_ascii_hexdigit())
        })
    } else {
        None
    };
    println!(
        "cargo:rustc-env=XFER_GIT_COMMIT={}",
        commit.as_deref().unwrap_or("unknown")
    );
}

fn main() {
    build_metadata();
    println!("cargo:rerun-if-changed=proto/xfer.proto");
    let mut config = prost_build::Config::new();
    config.skip_debug([
        ".xfer.ipc.v1.Request",
        ".xfer.ipc.v1.Command",
        ".xfer.ipc.v1.PasswordConnect",
        ".xfer.ipc.v1.OutputChunk",
    ]);
    // Compile the schema in Rust as well: normal Cargo builds do not need a
    // platform-specific protoc binary (important for the BSD targets).
    let descriptors = protox::compile(["proto/xfer.proto"], ["proto"]).expect("parse IPC schema");
    config
        .compile_fds(descriptors)
        .expect("generate IPC bindings");
}

//! Bookmarks are read-only to xfer; the user's editor owns formatting/comments.
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bookmark {
    #[serde(rename = "type", default)]
    pub protocol: String,
    pub url: String,
    #[serde(rename = "accessKey")]
    pub access_key: Option<String>,
    #[serde(rename = "secretKey")]
    pub secret_key: Option<String>,
    #[serde(rename = "sessionToken")]
    pub session_token: Option<String>,
    pub region: Option<String>,
    pub bucket: Option<String>,
    pub api: Option<String>,
    pub override_host: Option<String>,
    pub override_port: Option<u16>,
    /// SMB/WebDAV identity overrides; Some("") deliberately selects anonymous access.
    pub user: Option<String>,
    pub domain: Option<String>,
    pub password: Option<crate::credentials::Password>,
    #[serde(default)]
    pub password_interactive: bool,
}

impl Bookmark {
    /// Display only routing information. URL credentials and signed query strings
    /// are secrets too, so never print the original URL verbatim.
    pub fn address(&self) -> String {
        match url::Url::parse(&self.url) {
            Ok(mut url) => {
                let _ = url.set_password(None);
                let _ = url.set_username("");
                url.set_query(None);
                url.set_fragment(None);
                url.to_string()
                    .chars()
                    .flat_map(|ch| {
                        if ch.is_control() {
                            ch.escape_default().collect::<Vec<_>>()
                        } else {
                            vec![ch]
                        }
                    })
                    .collect()
            }
            Err(_) => "[invalid URL]".into(),
        }
    }
    pub fn summary(&self, alias: &str) -> String {
        let mut summary = format!("{}  {}", alias.escape_default(), self.address());
        if let Some(bucket) = &self.bucket {
            summary.push_str(&format!("  bucket={}", bucket.escape_default()));
        }
        summary
    }
}

/// Backends inferred from the URL scheme when `type` is omitted. Plain
/// `http`/`https` is genuinely ambiguous: S3-compatible endpoints and WebDAV
/// servers share it. Every valid S3-over-HTTPS bookmark carries `bucket` (or
/// `accessKey`), so those fields select S3 and everything else selects WebDAV.
/// An explicit `type` always wins; backend validation then rejects mismatches.
fn infer_protocol(name: &str, bookmark: &Bookmark) -> Result<String> {
    let Some((scheme, _)) = bookmark.url.split_once("://") else {
        bail!("bookmark {name}: url has no scheme; set type explicitly");
    };
    Ok(match scheme.to_ascii_lowercase().as_str() {
        "sftp" => "sftp",
        "s3" => "s3",
        "smb" => "smb",
        "dav" | "davs" | "webdav" | "webdavs" => "webdav",
        "http" | "https" => {
            if bookmark.bucket.is_some() || bookmark.access_key.is_some() {
                "s3"
            } else {
                "webdav"
            }
        }
        _ => bail!("bookmark {name}: unknown url scheme {scheme:?}; set type explicitly"),
    }
    .to_owned())
}

/// Bookmarks can carry credentials in the URL or dedicated fields, so any
/// debug output must use the same redaction as listings.
impl std::fmt::Debug for Bookmark {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Bookmark")
            .field("protocol", &self.protocol)
            .field("summary", &self.summary(""))
            .finish()
    }
}

pub fn default_path() -> PathBuf {
    crate::config_dir().join("bookmarks.toml")
}

pub fn load(path: &Path) -> Result<BTreeMap<String, Bookmark>> {
    let source = match fs::read_to_string(path) {
        Ok(source) => source,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(e) => return Err(e).context("read bookmarks"),
    };
    let source = zeroize::Zeroizing::new(source);
    let entries: BTreeMap<String, Bookmark> =
        toml::from_str(&source).map_err(|error| explain_parse_error(path, &source, &error))?;
    let mut entries = entries;
    for (name, entry) in entries.iter_mut() {
        if entry.protocol.trim().is_empty() {
            entry.protocol = infer_protocol(name, entry)?;
        }
    }
    for (name, entry) in &entries {
        if entry.password_interactive
            && !matches!(entry.protocol.as_str(), "sftp" | "smb" | "webdav")
        {
            bail!(
                "bookmark {name}: password_interactive is only supported for sftp, smb or webdav"
            );
        }
        if entry.password.is_some() && !matches!(entry.protocol.as_str(), "sftp" | "smb" | "webdav")
        {
            bail!("bookmark {name}: password is only supported for sftp, smb or webdav");
        }
        if entry.domain.is_some() && entry.protocol != "smb" {
            bail!("bookmark {name}: domain is only supported for smb");
        }
        if entry.user.is_some() && !matches!(entry.protocol.as_str(), "smb" | "webdav") {
            bail!("bookmark {name}: user is only supported for smb or webdav");
        }
        if entry.access_key.is_some() != entry.secret_key.is_some() {
            bail!("bookmark {name}: accessKey and secretKey must be provided together");
        }
    }
    Ok(entries)
}

/// Turn a TOML parse/decode failure into a diagnostic that stays safe when the
/// file carries credentials: forward only messages proven to contain field
/// names or structure words, and report positions instead of source content.
/// Serde's `invalid type`/`invalid value` render the offending value itself,
/// and raw parser errors embed source lines, so both stay suppressed; the
/// position still tells the user where to look.
fn explain_parse_error(path: &Path, source: &str, error: &toml::de::Error) -> anyhow::Error {
    let message = error.message();
    let name_only =
        message.starts_with("unknown field `") || message.starts_with("missing field `");
    let reason = if name_only {
        message
    } else if message.starts_with("invalid type:") || message.starts_with("invalid value:") {
        "a value has the wrong type"
    } else {
        "invalid or unexpected syntax"
    };
    anyhow::anyhow!("invalid bookmarks TOML at {}", path.display()).context(
        error
            .span()
            .map(|span| {
                let (line, column) = line_column(source, span.start);
                format!("{reason} at line {line}, column {column}")
            })
            .unwrap_or_else(|| reason.to_owned()),
    )
}

/// Translate a byte offset into 1-based line and column numbers. The offset is
/// bounded by the document length; parser spans always point inside it.
fn line_column(source: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(source.len());
    let line = source[..offset].matches('\n').count() + 1;
    let column = source[..offset]
        .rsplit_once('\n')
        .map_or(offset, |(_, last)| last.len())
        + 1;
    (line, column)
}

pub fn prepare_for_editor(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    // Never rewrite an existing file: comments and formatting remain untouched.
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn listing_omits_credentials_and_url_secrets() {
        let text = "minio = { type = 's3', url = 'https://user:password@localhost:9000/?token=secret#private', bucket = 'archive', accessKey = 'access-key', secretKey = 'secret-key' }";
        let entries: BTreeMap<String, Bookmark> = toml::from_str(text).unwrap();
        let summary = entries["minio"].summary("minio");
        assert_eq!(summary, "minio  https://localhost:9000/  bucket=archive");
    }

    #[test]
    fn preparing_existing_file_preserves_comments_and_contents() {
        let path =
            std::env::temp_dir().join(format!("xfer-bookmark-test-{}.toml", std::process::id()));
        let original = "# Keep this comment\nminio = { type = 's3', url = 'http://localhost' }\n";
        fs::write(&path, original).unwrap();
        prepare_for_editor(&path).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), original);
        assert!(load(&path).unwrap().contains_key("minio"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn interactive_password_is_opt_in() {
        let values: BTreeMap<String, Bookmark> = toml::from_str(
            "[server]\ntype = 'sftp'\nurl = 'sftp://user@host'\npassword_interactive = true\n[agent]\ntype = 'sftp'\nurl = 'sftp://alias'\n",
        ).unwrap();
        assert!(values["server"].password_interactive);
        assert!(!values["agent"].password_interactive);
    }

    #[test]
    fn smb_bookmarks_allow_the_same_out_of_band_password_workflow() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("bookmarks.toml");
        fs::write(&file, "[nas]\ntype = 'smb'\nurl = 'smb://WORKGROUP;alice@nas/share'\npassword_interactive = true\n").unwrap();
        assert!(load(&file).unwrap()["nas"].password_interactive);
        fs::write(
            &file,
            "[wrong]\ntype = 's3'\nurl = 's3://bucket'\npassword_interactive = true\n",
        )
        .unwrap();
        assert!(load(&file).is_err());
    }

    #[test]
    fn stored_credentials_and_explicit_empty_fields_are_preserved_but_never_listed() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("bookmarks.toml");
        fs::write(&file, "[vm]\ntype='smb'\nurl='smb://URL;user:url-secret@host/share'\nuser=''\ndomain=''\npassword=''\n[nas]\ntype='smb'\nurl='smb://host/share'\nuser='alice'\ndomain='DOMAIN'\npassword='book-secret'\n[ssh]\ntype='sftp'\nurl='sftp://host'\npassword='ssh-secret'\n").unwrap();
        let books = load(&file).unwrap();
        assert_eq!(books["vm"].user.as_deref(), Some(""));
        assert_eq!(books["vm"].domain.as_deref(), Some(""));
        assert_eq!(books["vm"].password.as_ref().unwrap().expose(), "");
        assert_eq!(
            books["nas"].password.as_ref().unwrap().expose(),
            "book-secret"
        );
        for (name, book) in books {
            let summary = book.summary(&name);
            for secret in ["url-secret", "book-secret", "ssh-secret"] {
                assert!(!summary.contains(secret));
            }
        }
    }

    #[test]
    fn missing_type_is_inferred_from_the_url_scheme_and_fields() {
        let text = "[a]\nurl='sftp://host'\n[b]\nurl='S3://bucket'\n[c]\nurl='smb://host/share'\n[d]\nurl='davs://host/files/'\n[e]\nurl='https://localhost:9000'\nbucket='archive'\naccessKey='k'\nsecretKey='s'\n[f]\nurl='https://dav.example.com/files/'\n";
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("bookmarks.toml");
        fs::write(&file, text).unwrap();
        let entries = load(&file).unwrap();
        assert_eq!(entries["a"].protocol, "sftp");
        assert_eq!(entries["b"].protocol, "s3");
        assert_eq!(entries["c"].protocol, "smb");
        assert_eq!(entries["d"].protocol, "webdav");
        assert_eq!(entries["e"].protocol, "s3");
        assert_eq!(entries["f"].protocol, "webdav");
        // Scheme detection stays textual; later backend validation rejects
        // invalid URLs such as queries and fragments.
        fs::write(&file, "[g]\nurl='https://host?query'\n").unwrap();
        assert_eq!(load(&file).unwrap()["g"].protocol, "webdav");
        fs::write(&file, "[h]\nurl='weird'\n").unwrap();
        assert!(load(&file)
            .unwrap_err()
            .to_string()
            .contains("set type explicitly"));
    }

    #[test]
    fn parse_named_tables_and_inline_tables() {
        let text = "# preserved by editor\nminio = { type = 's3', url = 'https://localhost:9000', accessKey = 'a', secretKey = 'b', api = 's3v4' }";
        let values: BTreeMap<String, Bookmark> = toml::from_str(text).unwrap();
        assert_eq!(values["minio"].protocol, "s3");
    }

    #[test]
    fn parse_errors_report_positions_without_leaking_values() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("bookmarks.toml");

        // A misspelled field is the common mistake: name it and place it.
        fs::write(
            &file,
            "[store01]\nurl = 'smb://store01'\nuser = 'HQ\\a'\npass = 'secret-value'\n",
        )
        .unwrap();
        let error = load(&file).unwrap_err().to_string();
        assert!(
            error.contains("unknown field `pass`, expected one of"),
            "{error}"
        );
        assert!(error.contains("line 4, column 1"), "{error}");
        assert!(!error.contains("secret-value"), "{error}");

        // A missing required field is named without quoting values.
        fs::write(&file, "[broken]\ntype = 'sftp'\n").unwrap();
        let error = load(&file).unwrap_err().to_string();
        assert!(error.contains("missing field `url`"), "{error}");

        // Type errors embed the offending value, so only the position is
        // forwarded.
        fs::write(&file, "[broken]\nurl = 123456789\n").unwrap();
        let error = load(&file).unwrap_err().to_string();
        assert!(error.contains("wrong type at line 2, column 7"), "{error}");
        assert!(!error.contains("123456789"), "{error}");

        // Raw syntax errors embed source lines; keep them summarized too.
        fs::write(&file, "[broken\nurl = 'value'\n").unwrap();
        let error = load(&file).unwrap_err().to_string();
        assert!(error.contains("line 1, column 8"), "{error}");
        assert!(!error.contains("value"), "{error}");

        // Inline-table values stay secret as well.
        fs::write(&file, "broken = { url = 'x', pass = 'secret-value' }\n").unwrap();
        let error = load(&file).unwrap_err().to_string();
        assert!(error.contains("unknown field `pass`"), "{error}");
        assert!(!error.contains("secret-value"), "{error}");
    }
}

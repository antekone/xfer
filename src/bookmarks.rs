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
    #[serde(rename = "type")]
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
    pub fn summary(&self, alias: &str) -> String {
        let endpoint = match url::Url::parse(&self.url) {
            Ok(mut url) => {
                let _ = url.set_password(None);
                let _ = url.set_username("");
                url.set_query(None);
                url.set_fragment(None);
                url.to_string()
            }
            Err(_) => "[invalid URL]".into(),
        };
        let mut summary = format!("{}  {}", alias.escape_default(), endpoint);
        if let Some(bucket) = &self.bucket {
            summary.push_str(&format!("  bucket={}", bucket.escape_default()));
        }
        summary
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
    // Do not print TOML parser errors: they can include credential-bearing source lines.
    let source = zeroize::Zeroizing::new(source);
    let entries: BTreeMap<String, Bookmark> = toml::from_str(&source).map_err(|_| {
        anyhow::anyhow!(
            "invalid bookmarks TOML at {} (expected named bookmark tables)",
            path.display()
        )
    })?;
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
    fn parse_named_tables_and_inline_tables() {
        let text = "# preserved by editor\nminio = { type = 's3', url = 'https://localhost:9000', accessKey = 'a', secretKey = 'b', api = 's3v4' }";
        let values: BTreeMap<String, Bookmark> = toml::from_str(text).unwrap();
        assert_eq!(values["minio"].protocol, "s3");
    }
}

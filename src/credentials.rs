/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! In-memory connection secrets, retained for reconnects until the connection is
//! dropped. Never put these in command text or job titles.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;
use zeroize::Zeroizing;

#[derive(Clone)]
pub struct Password(Arc<Zeroizing<String>>);
impl Password {
    pub fn new(value: String) -> Self {
        Self(Arc::new(Zeroizing::new(value)))
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for Password {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}

/// A URL credential shortcut must never be saved to command history.
pub(crate) fn sensitive_url(value: &str) -> bool {
    let Some((_, rest)) = value.split_once("://") else {
        return false;
    };
    if url::Url::parse(value).is_err() {
        return true;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    authority
        .rsplit_once('@')
        .is_some_and(|(user, _)| user.contains(':'))
        || value.contains('?')
        || value.contains('#')
}
/// Safe connection/job label, also for rejected or malformed credential URLs.
pub(crate) fn connection_label(value: &str) -> String {
    if !value.contains("://") {
        return value.escape_default().to_string();
    }
    match url::Url::parse(value) {
        Ok(mut url) => {
            let _ = url.set_password(None);
            let _ = url.set_username("");
            url.set_query(None);
            url.set_fragment(None);
            url.to_string().escape_default().to_string()
        }
        Err(_) => "[invalid URL]".into(),
    }
}
impl Serialize for Password {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.expose())
    }
}
impl<'de> Deserialize<'de> for Password {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::new)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasswordConnect {
    pub target: String,
    pub password: Password,
}

#[derive(Debug)]
pub struct PasswordRequired;
impl std::fmt::Display for PasswordRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("this bookmark requires an interactive password")
    }
}
impl std::error::Error for PasswordRequired {}

//! Ephemeral connection secrets. Never put these in command text or job titles.
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

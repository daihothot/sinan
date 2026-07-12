use std::{fmt, str::FromStr};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

pub const SUPPORTED_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1, 0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaVersion {
    pub major: u16,
    pub minor: u16,
}

impl SchemaVersion {
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    pub fn compatibility_with(
        self,
        supported: Self,
    ) -> Result<SchemaCompatibility, SchemaVersionError> {
        if self.major != supported.major {
            return Err(SchemaVersionError::MajorMismatch {
                received: self.major,
                supported: supported.major,
            });
        }

        Ok(match self.minor.cmp(&supported.minor) {
            std::cmp::Ordering::Less => SchemaCompatibility::OlderMinor,
            std::cmp::Ordering::Equal => SchemaCompatibility::Exact,
            std::cmp::Ordering::Greater => SchemaCompatibility::HigherMinor,
        })
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ecp.v{}.{}", self.major, self.minor)
    }
}

impl FromStr for SchemaVersion {
    type Err = SchemaVersionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let version = value
            .strip_prefix("ecp.v")
            .ok_or_else(|| SchemaVersionError::InvalidFormat(value.to_owned()))?;
        let (major, minor) = version
            .split_once('.')
            .ok_or_else(|| SchemaVersionError::InvalidFormat(value.to_owned()))?;

        if major.is_empty() || minor.is_empty() || minor.contains('.') {
            return Err(SchemaVersionError::InvalidFormat(value.to_owned()));
        }

        let major = major
            .parse::<u16>()
            .map_err(|_| SchemaVersionError::InvalidFormat(value.to_owned()))?;
        let minor = minor
            .parse::<u16>()
            .map_err(|_| SchemaVersionError::InvalidFormat(value.to_owned()))?;

        Ok(Self::new(major, minor))
    }
}

impl Serialize for SchemaVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for SchemaVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaCompatibility {
    Exact,
    OlderMinor,
    HigherMinor,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SchemaVersionError {
    #[error("invalid Execution Client Protocol schema version: {0}")]
    InvalidFormat(String),

    #[error("schema major mismatch: received {received}, supported {supported}")]
    MajorMismatch { received: u16, supported: u16 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_displays_schema_version() {
        let version: SchemaVersion = "ecp.v12.34".parse().unwrap();
        assert_eq!(version, SchemaVersion::new(12, 34));
        assert_eq!(version.to_string(), "ecp.v12.34");
    }

    #[test]
    fn rejects_malformed_versions() {
        for malformed in ["v1.0", "ecp.v1", "ecp.v1.0.1", "ecp.vx.0", "ecp.v1.x"] {
            assert!(malformed.parse::<SchemaVersion>().is_err(), "{malformed}");
        }
    }
}

use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::error::StoreError;

/// Compact JSON with recursively sorted object keys and its SHA-256 digest.
///
/// This representation is for persistence and duplicate detection. It is not
/// the Execution Client Protocol HMAC representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalJson {
    text: String,
    sha256_hex: String,
}

impl CanonicalJson {
    pub fn from_serializable<T: Serialize + ?Sized>(value: &T) -> Result<Self, StoreError> {
        Self::from_value(serde_json::to_value(value)?)
    }

    pub fn from_value(mut value: Value) -> Result<Self, StoreError> {
        sort_object_keys(&mut value);
        let text = serde_json::to_string(&value)?;
        let sha256_hex = sha256_hex(text.as_bytes());
        Ok(Self { text, sha256_hex })
    }

    pub fn parse(text: &str) -> Result<Self, StoreError> {
        Self::from_value(serde_json::from_str(text)?)
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    pub fn sha256_hex(&self) -> &str {
        &self.sha256_hex
    }

    pub fn into_parts(self) -> (String, String) {
        (self.text, self.sha256_hex)
    }

    pub(crate) fn from_stored(
        entity: &'static str,
        key: &str,
        text: String,
        stored_hash: String,
    ) -> Result<Self, StoreError> {
        let canonical = Self::parse(&text).map_err(|error| {
            StoreError::corrupt(entity, key, format!("invalid payload_json: {error}"))
        })?;

        if canonical.text != text {
            return Err(StoreError::corrupt(
                entity,
                key,
                "payload_json is not in canonical form",
            ));
        }
        if canonical.sha256_hex != stored_hash {
            return Err(StoreError::corrupt(
                entity,
                key,
                "payload_hash does not match payload_json",
            ));
        }

        Ok(canonical)
    }
}

fn sort_object_keys(value: &mut Value) {
    match value {
        Value::Array(values) => {
            for value in values {
                sort_object_keys(value);
            }
        }
        Value::Object(object) => {
            let mut entries: Vec<_> = std::mem::take(object).into_iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));

            let mut sorted = Map::new();
            for (key, mut value) in entries {
                sort_object_keys(&mut value);
                sorted.insert(key, value);
            }
            *object = sorted;
        }
        _ => {}
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::CanonicalJson;

    #[test]
    fn recursively_sorts_object_keys() {
        let first = CanonicalJson::from_value(json!({
            "z": [{"b": 2, "a": 1}],
            "a": {"d": 4, "c": 3}
        }))
        .expect("JSON should canonicalize");
        let second = CanonicalJson::parse(r#"{"a":{"c":3,"d":4},"z":[{"a":1,"b":2}]}"#)
            .expect("JSON should canonicalize");

        assert_eq!(first, second);
        assert_eq!(first.as_str(), r#"{"a":{"c":3,"d":4},"z":[{"a":1,"b":2}]}"#);
    }

    #[test]
    fn payload_change_changes_digest() {
        let first = CanonicalJson::from_value(json!({"value": 1})).unwrap();
        let second = CanonicalJson::from_value(json!({"value": 2})).unwrap();

        assert_ne!(first.sha256_hex(), second.sha256_hex());
    }
}

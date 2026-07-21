use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinan_types::ErrorCode;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpErrorResponse {
    pub error_code: ErrorCode,
    pub message: String,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    pub server_time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<BTreeMap<String, Value>>,
}

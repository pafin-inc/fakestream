//! AWS JSON 1.1 wire-protocol helpers shared by every operation.
//!
//! Kinesis speaks a single HTTP endpoint: `POST /` with the operation in the
//! `X-Amz-Target: Kinesis_20131202.<Op>` header, an `application/x-amz-json-1.1`
//! JSON body, and base64-encoded record payloads. Errors are HTTP 400 with a
//! `{"__type":..,"message":..}` body that both boto3 and aws-sdk-js v3 parse.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;

const TARGET_PREFIX: &str = "Kinesis_20131202.";

/// A typed AWS error: carries the exception name used in `__type` and the message.
#[derive(Debug)]
pub struct ApiError {
    pub kind: &'static str,
    pub message: String,
}

impl ApiError {
    pub fn new(kind: &'static str, message: impl Into<String>) -> Self {
        ApiError {
            kind,
            message: message.into(),
        }
    }

    pub fn not_found(what: impl Into<String>) -> Self {
        ApiError::new("ResourceNotFoundException", what)
    }

    pub fn validation(message: impl Into<String>) -> Self {
        ApiError::new("ValidationException", message)
    }

    pub fn body(&self) -> String {
        let payload = serde_json::json!({ "__type": self.kind, "message": self.message });
        payload.to_string()
    }
}

/// Extract the operation name from the `X-Amz-Target` header value.
pub fn parse_target(header: &str) -> Option<&str> {
    header.strip_prefix(TARGET_PREFIX)
}

pub fn encode_data(bytes: &[u8]) -> String {
    B64.encode(bytes)
}

/// Append the base64 encoding of `bytes` directly into `out`, with no
/// intermediate `String` allocation. The GetRecords serializer uses this to
/// write record payloads straight into the response buffer. base64's alphabet
/// contains no JSON-escape characters, so the output is safe between quotes.
pub fn encode_data_into(bytes: &[u8], out: &mut Vec<u8>) {
    let need = bytes.len().div_ceil(3) * 4; // standard padded base64 length
    let start = out.len();
    out.resize(start + need, 0);
    let written = B64
        .encode_slice(bytes, &mut out[start..])
        .expect("output buffer is exactly pre-sized for base64");
    debug_assert_eq!(written, need);
}

pub fn decode_data(text: &str) -> Result<Vec<u8>, ApiError> {
    B64.decode(text)
        .map_err(|_| ApiError::new("InvalidArgumentException", "Data is not valid base64"))
}

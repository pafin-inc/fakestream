//! One handler per supported Kinesis operation.
//!
//! Scope is deliberately exactly what a polling-consumer setup exercises plus
//! the cheap essentials (PutRecords, DeleteStream, DescribeStreamSummary). Not
//! implemented on purpose: SubscribeToShard / enhanced fan-out (consumers
//! poll), resharding (single-shard assumption), tagging, encryption, KCL.

use serde_json::{json, Value};

use crate::protocol::{decode_data, encode_data, encode_data_into, ApiError};
use crate::store::{now_ms, Iterator, Record, Shard, Store, Stream};
use crate::wal::Wal;

const MAX_GET_RECORDS: u64 = 10_000;
const MAX_GET_RECORDS_BYTES: usize = 10 * 1_048_576; // 10 MiB, Kinesis GetRecords response cap
const RECORD_JSON_OVERHEAD: usize = 144; // fixed keys + quotes + seq/timestamp digits + comma
const MAX_RECORD_BYTES: usize = 1_048_576; // 1 MiB, Kinesis data-blob limit (decoded)
const MAX_PUT_RECORDS_COUNT: usize = 500;
const MAX_SHARD_COUNT: u64 = 10_000; // local sanity cap; also blocks the old `as u32` truncation
const MAX_PUT_RECORDS_AGGREGATE_BYTES: usize = 5 * 1_048_576; // 5 MiB, data + partition keys

fn require_str<'a>(req: &'a Value, key: &str) -> Result<&'a str, ApiError> {
    req.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::validation(format!("Missing required parameter: {key}")))
}

/// Extract the stream name from a `StreamARN` (the segment after the last `/`
/// of an ARN that contains ":stream/").
fn arn_stream_name(arn: &str) -> Option<&str> {
    arn.contains(":stream/")
        .then(|| arn.rsplit('/').next())
        .flatten()
        .filter(|name| !name.is_empty())
}

/// Resolve the target stream name from a data-plane request. Since the 2022
/// Kinesis API update these ops accept `StreamARN` in place of `StreamName`;
/// clients commonly feed back the ARN returned by DescribeStream/ListStreams.
/// Prefer `StreamName`; when both are supplied they must agree, matching real
/// Kinesis, rather than silently ignoring a mismatched ARN.
fn resolve_stream_name(req: &Value) -> Result<&str, ApiError> {
    let name = req.get("StreamName").and_then(Value::as_str);
    let arn = req.get("StreamARN").and_then(Value::as_str);
    match (name, arn) {
        (Some(name), Some(arn)) => {
            let arn_name = arn_stream_name(arn)
                .ok_or_else(|| ApiError::validation(format!("Invalid StreamARN: {arn}")))?;
            if arn_name != name {
                return Err(ApiError::validation(format!(
                    "StreamARN {arn} does not match StreamName {name}"
                )));
            }
            Ok(name)
        }
        (Some(name), None) => Ok(name),
        (None, Some(arn)) => arn_stream_name(arn)
            .ok_or_else(|| ApiError::validation(format!("Invalid StreamARN: {arn}"))),
        (None, None) => Err(ApiError::validation(
            "Missing required parameter: StreamName",
        )),
    }
}

fn parse_u128(text: &str, field: &str) -> Result<u128, ApiError> {
    text.parse::<u128>()
        .map_err(|_| ApiError::validation(format!("Invalid {field}: {text}")))
}

fn hash_key_range(shard: &Shard) -> Value {
    json!({
        "StartingHashKey": shard.hash_start.to_string(),
        "EndingHashKey": shard.hash_end.to_string(),
    })
}

fn shard_json(shard: &Shard) -> Value {
    let starting = shard.records.first().map_or(0, |r| r.seq);
    json!({
        "ShardId": shard.id,
        "HashKeyRange": hash_key_range(shard),
        "SequenceNumberRange": { "StartingSequenceNumber": starting.to_string() },
    })
}

fn encode_iterator(it: &Iterator) -> String {
    let payload = json!({
        "stream": it.stream,
        "shard_id": it.shard_id,
        "next_seq": it.next_seq,
        "expires_ms": it.expires_ms.to_string(),
    });
    encode_data(payload.to_string().as_bytes())
}

fn decode_iterator(token: &str) -> Result<Iterator, ApiError> {
    let bytes = decode_data(token)
        .map_err(|_| ApiError::new("InvalidArgumentException", "Malformed shard iterator"))?;
    let v: Value = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::new("InvalidArgumentException", "Malformed shard iterator"))?;
    let expires_ms = v
        .get("expires_ms")
        .and_then(Value::as_str)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    Ok(Iterator {
        stream: v
            .get("stream")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        shard_id: v
            .get("shard_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        next_seq: v.get("next_seq").and_then(Value::as_u64).unwrap_or(0),
        expires_ms,
    })
}

// ---- write operations -------------------------------------------------------

/// Enforce the Kinesis stream-name constraint `[a-zA-Z0-9_.-]{1,128}`. Also
/// guards the Prometheus exposition, where the name is interpolated into a label.
fn validate_stream_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > 128 {
        return Err(ApiError::validation(format!(
            "1 validation error detected: Value '{name}' at 'streamName' failed to satisfy \
             constraint: Member must have length between 1 and 128"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    {
        return Err(ApiError::validation(format!(
            "1 validation error detected: Value '{name}' at 'streamName' failed to satisfy \
             constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
        )));
    }
    Ok(())
}

pub fn create_stream(store: &mut Store, req: &Value) -> Result<Value, ApiError> {
    let name = require_str(req, "StreamName")?;
    validate_stream_name(name)?;
    let shard_count = match req.get("ShardCount") {
        None => 1u32,
        Some(value) => value
            .as_u64()
            .filter(|count| (1..=MAX_SHARD_COUNT).contains(count))
            .ok_or_else(|| {
                ApiError::validation(format!(
                    "ShardCount must be an integer between 1 and {MAX_SHARD_COUNT}"
                ))
            })? as u32,
    };
    let retention_secs = match req.get("RetentionPeriodHours") {
        None => None,
        Some(value) => {
            let hours = value.as_u64().ok_or_else(|| {
                ApiError::validation("RetentionPeriodHours must be a non-negative integer")
            })?;
            Some(hours.checked_mul(3600).ok_or_else(|| {
                ApiError::validation("RetentionPeriodHours is too large to represent")
            })?)
        }
    };
    if !store.create_stream(name, shard_count, retention_secs) {
        return Err(ApiError::new(
            "ResourceInUseException",
            format!("Stream {name} already exists"),
        ));
    }
    Ok(json!({}))
}

pub fn delete_stream(store: &mut Store, req: &Value) -> Result<Value, ApiError> {
    let name = resolve_stream_name(req)?;
    if store.streams.remove(name).is_none() {
        return Err(ApiError::not_found(format!("Stream {name} not found")));
    }
    Ok(json!({}))
}

pub fn put_record(
    store: &mut Store,
    wal: Option<&mut Wal>,
    req: &Value,
) -> Result<Value, ApiError> {
    let name = resolve_stream_name(req)?;
    let partition_key = require_str(req, "PartitionKey")?.to_string();
    let data = decode_data(require_str(req, "Data")?)?;
    if data.len() > MAX_RECORD_BYTES {
        return Err(ApiError::validation(
            "1 validation error detected: Value at 'data' failed to satisfy constraint: \
             Member must have length less than or equal to 1048576",
        ));
    }
    let explicit = match req.get("ExplicitHashKey").and_then(Value::as_str) {
        Some(text) => Some(parse_u128(text, "ExplicitHashKey")?),
        None => None,
    };
    let (shard_id, seq) = store
        .put(name, partition_key, data, explicit)
        .ok_or_else(|| ApiError::not_found(format!("Stream {name} not found")))?;
    if let Some(wal) = wal {
        let appended = store
            .last_record(name, &shard_id)
            .map(|rec| wal.append(name, &shard_id, rec));
        if let Some(Err(err)) = appended {
            // The record wasn't durably written; roll it back so we don't ack a
            // seq that vanishes on restart (which would let the seq high-water
            // regress and re-issue that number for a different record).
            eprintln!("fakestream: WAL append failed: {err}");
            store.pop_record(name, &shard_id, seq);
            return Err(ApiError::internal("Record could not be durably persisted"));
        }
    }
    Ok(json!({ "ShardId": shard_id, "SequenceNumber": seq.to_string() }))
}

pub fn put_records(
    store: &mut Store,
    mut wal: Option<&mut Wal>,
    req: &Value,
) -> Result<Value, ApiError> {
    let name = resolve_stream_name(req)?;
    if !store.streams.contains_key(name) {
        return Err(ApiError::not_found(format!("Stream {name} not found")));
    }
    let records = req
        .get("Records")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::validation("Missing required parameter: Records"))?;
    if records.len() > MAX_PUT_RECORDS_COUNT {
        return Err(ApiError::validation(format!(
            "1 validation error detected: Value at 'records' failed to satisfy constraint: \
             Member must have length less than or equal to {MAX_PUT_RECORDS_COUNT}"
        )));
    }

    // Decode + validate the whole request before storing anything (Kinesis is atomic).
    let mut decoded = Vec::with_capacity(records.len());
    let mut aggregate = 0usize;
    for (i, record) in records.iter().enumerate() {
        let partition_key = require_str(record, "PartitionKey")?.to_string();
        let data = decode_data(require_str(record, "Data")?)?;
        if data.len() > MAX_RECORD_BYTES {
            return Err(ApiError::validation(format!(
                "1 validation error detected: Value at 'records.{i}.member.data' failed to \
                 satisfy constraint: Member must have length less than or equal to 1048576"
            )));
        }
        aggregate += data.len() + partition_key.len();
        let explicit = match record.get("ExplicitHashKey").and_then(Value::as_str) {
            Some(text) => Some(parse_u128(text, "ExplicitHashKey")?),
            None => None,
        };
        decoded.push((partition_key, data, explicit));
    }
    if aggregate > MAX_PUT_RECORDS_AGGREGATE_BYTES {
        return Err(ApiError::validation(
            "Records size exceeds the maximum allowed of 5 MiB for the entire request",
        ));
    }

    let mut out = Vec::with_capacity(decoded.len());
    let mut failed = 0u64;
    for (partition_key, data, explicit) in decoded {
        // The stream exists (checked above) and shards span the full hash ring
        // under a single write lock, so put always routes to a shard.
        let Some((shard_id, seq)) = store.put(name, partition_key, data, explicit) else {
            return Err(ApiError::not_found(format!("Stream {name} not found")));
        };
        if let Some(wal) = wal.as_deref_mut() {
            let appended = store
                .last_record(name, &shard_id)
                .map(|rec| wal.append(name, &shard_id, rec));
            if let Some(Err(err)) = appended {
                // Per-record failure: roll the record back and report it so the
                // client can retry, mirroring real Kinesis partial-failure batches.
                eprintln!("fakestream: WAL append failed: {err}");
                store.pop_record(name, &shard_id, seq);
                failed += 1;
                out.push(json!({
                    "ErrorCode": "InternalFailure",
                    "ErrorMessage": "Record could not be durably persisted",
                }));
                continue;
            }
        }
        out.push(json!({ "ShardId": shard_id, "SequenceNumber": seq.to_string() }));
    }
    Ok(json!({ "FailedRecordCount": failed, "Records": out }))
}

// ---- read operations --------------------------------------------------------

fn lookup<'a>(store: &'a Store, name: &str) -> Result<&'a Stream, ApiError> {
    store
        .streams
        .get(name)
        .ok_or_else(|| ApiError::not_found(format!("Stream {name} not found")))
}

pub fn list_streams(store: &Store, _req: &Value) -> Result<Value, ApiError> {
    let mut names: Vec<&String> = store.streams.keys().collect();
    names.sort();
    let summaries: Vec<Value> = names
        .iter()
        .map(|n| {
            let s = &store.streams[*n];
            json!({ "StreamName": s.name, "StreamARN": s.arn, "StreamStatus": "ACTIVE" })
        })
        .collect();
    Ok(json!({ "StreamNames": names, "StreamSummaries": summaries, "HasMoreStreams": false }))
}

pub fn describe_stream(store: &Store, req: &Value) -> Result<Value, ApiError> {
    let stream = lookup(store, resolve_stream_name(req)?)?;
    let shards: Vec<Value> = stream.shards.iter().map(shard_json).collect();
    Ok(json!({
        "StreamDescription": {
            "StreamName": stream.name,
            "StreamARN": stream.arn,
            "StreamStatus": "ACTIVE",
            "StreamModeDetails": { "StreamMode": "PROVISIONED" },
            "RetentionPeriodHours": stream.retention_secs / 3600,
            "StreamCreationTimestamp": (stream.created_ms / 1000) as f64,
            "EncryptionType": "NONE",
            "EnhancedMonitoring": [],
            "HasMoreShards": false,
            "Shards": shards,
        }
    }))
}

pub fn describe_stream_summary(store: &Store, req: &Value) -> Result<Value, ApiError> {
    let stream = lookup(store, resolve_stream_name(req)?)?;
    let open = stream.shards.len();
    Ok(json!({
        "StreamDescriptionSummary": {
            "StreamName": stream.name,
            "StreamARN": stream.arn,
            "StreamStatus": "ACTIVE",
            "StreamModeDetails": { "StreamMode": "PROVISIONED" },
            "RetentionPeriodHours": stream.retention_secs / 3600,
            "StreamCreationTimestamp": (stream.created_ms / 1000) as f64,
            "EncryptionType": "NONE",
            "OpenShardCount": open,
            "EnhancedMonitoring": [],
        }
    }))
}

pub fn list_shards(store: &Store, req: &Value) -> Result<Value, ApiError> {
    let stream = lookup(store, resolve_stream_name(req)?)?;
    let shards: Vec<Value> = stream.shards.iter().map(shard_json).collect();
    Ok(json!({ "Shards": shards }))
}

pub fn get_shard_iterator(store: &Store, req: &Value) -> Result<Value, ApiError> {
    let name = resolve_stream_name(req)?;
    let shard_id = require_str(req, "ShardId")?;
    let iterator_type = require_str(req, "ShardIteratorType")?;
    let starting =
        match req.get("StartingSequenceNumber").and_then(Value::as_str) {
            Some(text) => Some(text.parse::<u64>().map_err(|_| {
                ApiError::new("ValidationException", "Invalid StartingSequenceNumber")
            })?),
            None => None,
        };
    let timestamp_ms = req
        .get("Timestamp")
        .and_then(Value::as_f64)
        .map(|secs| (secs * 1000.0) as u128);
    // Classify argument errors before touching the store so a bad iterator type
    // or a missing StartingSequenceNumber doesn't masquerade as a deleted shard.
    match iterator_type {
        "TRIM_HORIZON" | "LATEST" => {}
        "AT_TIMESTAMP" => {
            if timestamp_ms.is_none() {
                return Err(ApiError::new(
                    "InvalidArgumentException",
                    "Timestamp is required for ShardIteratorType AT_TIMESTAMP",
                ));
            }
        }
        "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER" => {
            if starting.is_none() {
                return Err(ApiError::new(
                    "InvalidArgumentException",
                    format!(
                        "StartingSequenceNumber is required for ShardIteratorType {iterator_type}"
                    ),
                ));
            }
        }
        other => {
            return Err(ApiError::validation(format!(
                "Invalid ShardIteratorType: {other}"
            )));
        }
    }
    lookup(store, name)?;
    let it = store
        .make_iterator(name, shard_id, iterator_type, starting, timestamp_ms)
        .ok_or_else(|| ApiError::not_found(format!("Shard {shard_id} not found in {name}")))?;
    Ok(json!({ "ShardIterator": encode_iterator(&it) }))
}

/// Returns `(serialized JSON body, record count, raw payload bytes)`. The body
/// is built straight into one pre-sized buffer with base64 written in place — no
/// `serde_json::Value` tree, no per-record `String`. The count/bytes are
/// reported to metrics by the caller (exact, not estimated from the wire form).
pub fn get_records(store: &Store, req: &Value) -> Result<(Vec<u8>, u64, u64), ApiError> {
    let it = decode_iterator(require_str(req, "ShardIterator")?)?;
    if now_ms() > it.expires_ms {
        return Err(ApiError::new(
            "ExpiredIteratorException",
            "Iterator expired (5 min TTL)",
        ));
    }
    let limit = match req.get("Limit") {
        None => MAX_GET_RECORDS as usize,
        Some(value) => value
            .as_u64()
            .filter(|limit| (1..=MAX_GET_RECORDS).contains(limit))
            .ok_or_else(|| {
                ApiError::validation(
                    "1 validation error detected: Value at 'limit' failed to satisfy \
                     constraint: Member must have value between 1 and 10000",
                )
            })? as usize,
    };
    let stream = lookup(store, &it.stream)?;
    let shard = stream
        .shards
        .iter()
        .find(|s| s.id == it.shard_id)
        .ok_or_else(|| ApiError::not_found(format!("Shard {} not found", it.shard_id)))?;

    // Records are append-ordered by monotonic seq (WAL replay preserves order),
    // so binary-search the first un-consumed record instead of scanning the
    // already-consumed prefix on every poll.
    let start = shard.records.partition_point(|r| r.seq < it.next_seq);
    let mut selected: Vec<&Record> = Vec::new();
    let mut payload_bytes = 0usize;
    let mut next_seq = it.next_seq;
    let mut last_ts: Option<u128> = None;
    for record in &shard.records[start..] {
        if selected.len() >= limit {
            break;
        }
        // Bound the response at 10 MiB of payload, but always return at least one
        // record so an oversize record still makes progress.
        if !selected.is_empty() && payload_bytes + record.data.len() > MAX_GET_RECORDS_BYTES {
            break;
        }
        payload_bytes += record.data.len();
        next_seq = record.seq + 1;
        last_ts = Some(record.timestamp_ms);
        selected.push(record);
    }

    let latest_seq = shard.records.last().map_or(0, |r| r.seq);
    let millis_behind = if next_seq > latest_seq {
        0
    } else {
        last_ts.map_or(0, |ts| now_ms().saturating_sub(ts) as u64)
    };
    let next = Iterator {
        next_seq,
        expires_ms: now_ms() + 300_000,
        ..it
    };
    let body = serialize_get_records(&selected, &encode_iterator(&next), millis_behind);
    Ok((body, selected.len() as u64, payload_bytes as u64))
}

/// Serialize a GetRecords response into one pre-sized `Vec<u8>`. The capacity is
/// a guaranteed over-estimate so the buffer never reallocates mid-build, keeping
/// the response a single predictable allocation.
fn serialize_get_records(records: &[&Record], next_iterator: &str, millis_behind: u64) -> Vec<u8> {
    let mut capacity = next_iterator.len() + 64;
    for record in records {
        // pk * 6: worst-case JSON escaping (a control char expands to `\uXXXX`).
        // data: exact padded base64 length.
        capacity += RECORD_JSON_OVERHEAD
            + record.partition_key.len() * 6
            + record.data.len().div_ceil(3) * 4;
    }
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(b"{\"Records\":[");
    for (i, record) in records.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        write_record(&mut out, record);
    }
    out.extend_from_slice(b"],\"NextShardIterator\":");
    write_json_string(&mut out, next_iterator);
    out.extend_from_slice(b",\"MillisBehindLatest\":");
    out.extend_from_slice(millis_behind.to_string().as_bytes());
    out.push(b'}');
    debug_assert!(
        out.len() <= capacity,
        "GetRecords buffer estimate too small: {} > {}",
        out.len(),
        capacity
    );
    out
}

/// Write a single record object, base64-encoding its data straight into `out`.
fn write_record(out: &mut Vec<u8>, record: &Record) {
    out.extend_from_slice(b"{\"SequenceNumber\":\"");
    out.extend_from_slice(record.seq.to_string().as_bytes());
    out.extend_from_slice(b"\",\"ApproximateArrivalTimestamp\":");
    // Serialize the f64 through serde_json so the numeric form matches AWS SDKs
    // exactly (e.g. trailing `.0`).
    let timestamp = (record.timestamp_ms / 1000) as f64;
    serde_json::to_writer(&mut *out, &timestamp).expect("f64 serialization is infallible");
    out.extend_from_slice(b",\"PartitionKey\":");
    write_json_string(out, &record.partition_key);
    out.extend_from_slice(b",\"Data\":\"");
    encode_data_into(&record.data, out);
    out.extend_from_slice(b"\"}");
}

/// Write a properly-escaped JSON string (including the surrounding quotes).
fn write_json_string(out: &mut Vec<u8>, value: &str) {
    serde_json::to_writer(&mut *out, value).expect("string serialization is infallible");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::encode_data;
    use proptest::prelude::*;

    fn store_with_stream() -> Store {
        let mut s = Store::new(86_400);
        s.create_stream("S", 1, None);
        s
    }

    fn put_req(data_bytes: usize) -> Value {
        json!({ "StreamName": "S", "PartitionKey": "p", "Data": encode_data(&vec![0u8; data_bytes]) })
    }

    fn records_req(records: Vec<(usize, usize)>) -> Value {
        // each entry = (partition_key_len, data_len)
        let recs: Vec<Value> = records
            .into_iter()
            .map(|(pk, d)| json!({ "PartitionKey": "x".repeat(pk), "Data": encode_data(&vec![0u8; d]) }))
            .collect();
        json!({ "StreamName": "S", "Records": recs })
    }

    #[test]
    fn put_records_rejects_too_many() {
        let mut store = store_with_stream();
        let req = records_req(vec![(1, 1); MAX_PUT_RECORDS_COUNT + 1]);
        assert_eq!(
            put_records(&mut store, None, &req).unwrap_err().kind,
            "ValidationException"
        );
    }

    #[test]
    fn put_records_rejects_oversized_member_without_storing() {
        let mut store = store_with_stream();
        let req = records_req(vec![(1, 10), (1, MAX_RECORD_BYTES + 1)]);
        assert_eq!(
            put_records(&mut store, None, &req).unwrap_err().kind,
            "ValidationException"
        );
        // atomic: nothing stored
        assert_eq!(
            store.stream_sizes().iter().map(|(_, n, _)| n).sum::<u64>(),
            0
        );
    }

    #[test]
    fn put_records_rejects_over_5mib_aggregate() {
        let mut store = store_with_stream();
        let req = records_req(vec![(1, 900_000); 6]); // 5.4 MB total, each < 1 MiB, count < 500
        assert_eq!(
            put_records(&mut store, None, &req).unwrap_err().kind,
            "ValidationException"
        );
    }

    #[test]
    fn put_records_accepts_valid_batch() {
        let mut store = store_with_stream();
        let req = records_req(vec![(4, 1000); 10]);
        let out = put_records(&mut store, None, &req).unwrap();
        assert_eq!(out["FailedRecordCount"], 0);
    }

    #[test]
    fn put_record_rejects_over_1mib() {
        let mut store = store_with_stream();
        let err = put_record(&mut store, None, &put_req(MAX_RECORD_BYTES + 1)).unwrap_err();
        assert_eq!(err.kind, "ValidationException");
    }

    #[test]
    fn put_record_accepts_exactly_1mib() {
        let mut store = store_with_stream();
        assert!(put_record(&mut store, None, &put_req(MAX_RECORD_BYTES)).is_ok());
    }

    #[test]
    fn put_records_rejects_unknown_stream() {
        let mut store = Store::new(86_400); // NO stream created
        let req = json!({ "StreamName": "MISSING", "Records": [
            { "PartitionKey": "p", "Data": encode_data(&[1, 2, 3]) }
        ]});
        assert_eq!(
            put_records(&mut store, None, &req).unwrap_err().kind,
            "ResourceNotFoundException"
        );
    }

    #[test]
    fn put_record_appends_to_wal() {
        use crate::wal::Wal;
        let dir = std::env::temp_dir().join(format!("fs-ops-wal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut store = store_with_stream(); // stream "S"
        let (mut wal, _) = Wal::load(&dir, 1 << 20).unwrap();
        put_record(&mut store, Some(&mut wal), &put_req(10)).unwrap();
        drop(wal);
        let (_w, entries) = Wal::load(&dir, 1 << 20).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "S"); // stream name recorded
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_records_reports_internal_failure_when_append_fails() {
        use crate::wal::Wal;
        let dir = std::env::temp_dir().join(format!("fs-ops-walfail-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut store = store_with_stream();
        // segment_max 0 makes every append roll first; removing the wal dir then
        // makes the roll's create-open fail, forcing a WAL append error.
        let (mut wal, _) = Wal::load(&dir, 0).unwrap();
        std::fs::remove_dir_all(dir.join("wal")).unwrap();
        let req = records_req(vec![(1, 4)]);
        let out = put_records(&mut store, Some(&mut wal), &req).unwrap();
        assert_eq!(out["FailedRecordCount"], 1);
        assert_eq!(out["Records"][0]["ErrorCode"], "InternalFailure");
        // Rolled back: the un-persisted record is not left in the store.
        assert_eq!(
            store.stream_sizes().iter().map(|(_, n, _)| n).sum::<u64>(),
            0
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn describe_stream_accepts_stream_arn() {
        let store = store_with_stream();
        let arn = store.streams["S"].arn.clone();
        let out = describe_stream(&store, &json!({ "StreamARN": arn })).unwrap();
        assert_eq!(out["StreamDescription"]["StreamName"], "S");
    }

    #[test]
    fn describe_stream_rejects_malformed_arn() {
        let store = store_with_stream();
        assert_eq!(
            describe_stream(&store, &json!({ "StreamARN": "not-an-arn" }))
                .unwrap_err()
                .kind,
            "ValidationException"
        );
    }

    #[test]
    fn describe_stream_rejects_arn_name_mismatch() {
        let store = store_with_stream();
        let arn = store.streams["S"].arn.clone();
        assert_eq!(
            describe_stream(&store, &json!({ "StreamName": "other", "StreamARN": arn }))
                .unwrap_err()
                .kind,
            "ValidationException"
        );
    }

    #[test]
    fn describe_stream_accepts_matching_name_and_arn() {
        let store = store_with_stream();
        let arn = store.streams["S"].arn.clone();
        let out = describe_stream(&store, &json!({ "StreamName": "S", "StreamARN": arn })).unwrap();
        assert_eq!(out["StreamDescription"]["StreamName"], "S");
    }

    // ---- CreateStream name validation ------------------------------------------

    fn create(name: &str) -> Result<Value, ApiError> {
        create_stream(&mut Store::new(86_400), &json!({ "StreamName": name }))
    }

    #[test]
    fn create_stream_accepts_128_char_name() {
        assert!(create(&"a".repeat(128)).is_ok());
    }

    #[test]
    fn create_stream_rejects_129_char_name() {
        assert_eq!(
            create(&"a".repeat(129)).unwrap_err().kind,
            "ValidationException"
        );
    }

    #[test]
    fn create_stream_rejects_illegal_characters() {
        assert_eq!(create("bad\"name").unwrap_err().kind, "ValidationException");
    }

    #[test]
    fn create_stream_rejects_empty_name() {
        assert_eq!(create("").unwrap_err().kind, "ValidationException");
    }

    fn create_full(req: &Value) -> Result<Value, ApiError> {
        create_stream(&mut Store::new(86_400), req)
    }

    #[test]
    fn create_stream_accepts_shard_count_boundaries() {
        assert!(create_full(&json!({ "StreamName": "S", "ShardCount": 1 })).is_ok());
        assert!(create_full(&json!({ "StreamName": "S", "ShardCount": 10_000 })).is_ok());
    }

    #[test]
    fn create_stream_rejects_bad_shard_count() {
        for value in [json!(0), json!(10_001), json!(1.5)] {
            assert_eq!(
                create_full(&json!({ "StreamName": "S", "ShardCount": value }))
                    .unwrap_err()
                    .kind,
                "ValidationException"
            );
        }
    }

    #[test]
    fn create_stream_accepts_short_retention() {
        let out = create_full(&json!({ "StreamName": "S", "RetentionPeriodHours": 1 }));
        assert!(out.is_ok());
    }

    #[test]
    fn create_stream_rejects_overflowing_retention() {
        assert_eq!(
            create_full(&json!({ "StreamName": "S", "RetentionPeriodHours": u64::MAX }))
                .unwrap_err()
                .kind,
            "ValidationException"
        );
    }

    #[test]
    fn create_stream_rejects_mistyped_retention() {
        for value in [json!(-1), json!(1.5), json!("24")] {
            assert_eq!(
                create_full(&json!({ "StreamName": "S", "RetentionPeriodHours": value }))
                    .unwrap_err()
                    .kind,
                "ValidationException"
            );
        }
    }

    // ---- GetShardIterator argument classification ------------------------------

    fn iterator_req(shard_id: &str, iterator_type: &str) -> Value {
        json!({ "StreamName": "S", "ShardId": shard_id, "ShardIteratorType": iterator_type })
    }

    #[test]
    fn get_shard_iterator_rejects_unknown_type() {
        let store = store_with_stream();
        let req = iterator_req("shardId-000000000000", "BOGUS");
        assert_eq!(
            get_shard_iterator(&store, &req).unwrap_err().kind,
            "ValidationException"
        );
    }

    #[test]
    fn get_shard_iterator_requires_sequence_number() {
        let store = store_with_stream();
        let req = iterator_req("shardId-000000000000", "AT_SEQUENCE_NUMBER");
        assert_eq!(
            get_shard_iterator(&store, &req).unwrap_err().kind,
            "InvalidArgumentException"
        );
    }

    #[test]
    fn get_shard_iterator_requires_timestamp() {
        let store = store_with_stream();
        let req = iterator_req("shardId-000000000000", "AT_TIMESTAMP");
        assert_eq!(
            get_shard_iterator(&store, &req).unwrap_err().kind,
            "InvalidArgumentException"
        );
    }

    #[test]
    fn get_shard_iterator_unknown_shard_is_not_found() {
        let store = store_with_stream();
        let req = iterator_req("shardId-000000000099", "TRIM_HORIZON");
        assert_eq!(
            get_shard_iterator(&store, &req).unwrap_err().kind,
            "ResourceNotFoundException"
        );
    }

    // ---- GetRecords serialization + 10 MiB cap ---------------------------------

    fn iterator_token(store: &Store, iterator_type: &str) -> String {
        let req = json!({
            "StreamName": "S",
            "ShardId": "shardId-000000000000",
            "ShardIteratorType": iterator_type,
        });
        get_shard_iterator(store, &req).unwrap()["ShardIterator"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn get(store: &Store, token: &str, limit: Option<u64>) -> (Value, u64, u64) {
        let mut req = json!({ "ShardIterator": token });
        if let Some(l) = limit {
            req["Limit"] = json!(l);
        }
        let (body, count, bytes) = get_records(store, &req).unwrap();
        (serde_json::from_slice(&body).unwrap(), count, bytes)
    }

    #[test]
    fn get_records_serializes_fields_and_counts() {
        let mut store = store_with_stream();
        store.put("S", "pk-1".into(), vec![1, 2, 3, 4], None);
        store.put("S", "pk-2".into(), vec![5, 6, 7], None);
        let token = iterator_token(&store, "TRIM_HORIZON");
        let (v, count, bytes) = get(&store, &token, None);
        assert_eq!(count, 2);
        assert_eq!(bytes, 7);
        let recs = v["Records"].as_array().unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0]["PartitionKey"], "pk-1");
        assert_eq!(recs[0]["Data"], encode_data(&[1, 2, 3, 4]));
        assert_eq!(recs[1]["Data"], encode_data(&[5, 6, 7]));
        assert!(recs[0]["SequenceNumber"].is_string());
        assert!(recs[0]["ApproximateArrivalTimestamp"].is_number());
        assert!(v["NextShardIterator"].is_string());
        assert_eq!(v["MillisBehindLatest"], 0);
    }

    #[test]
    fn get_records_respects_limit() {
        let mut store = store_with_stream();
        for i in 0..5 {
            store.put("S", format!("p{i}"), vec![0u8; 10], None);
        }
        let token = iterator_token(&store, "TRIM_HORIZON");
        let (v, count, _) = get(&store, &token, Some(2));
        assert_eq!(count, 2);
        assert_eq!(v["Records"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn get_records_accepts_limit_boundaries() {
        let store = store_with_stream();
        let token = iterator_token(&store, "TRIM_HORIZON");
        for limit in [1u64, MAX_GET_RECORDS] {
            let req = json!({ "ShardIterator": token, "Limit": limit });
            assert!(get_records(&store, &req).is_ok());
        }
    }

    #[test]
    fn get_records_rejects_out_of_range_limit() {
        let store = store_with_stream();
        let token = iterator_token(&store, "TRIM_HORIZON");
        for limit in [json!(0), json!(MAX_GET_RECORDS + 1)] {
            let req = json!({ "ShardIterator": token, "Limit": limit });
            assert_eq!(
                get_records(&store, &req).unwrap_err().kind,
                "ValidationException"
            );
        }
    }

    #[test]
    fn get_records_caps_response_at_10_mib_and_resumes() {
        let mut store = store_with_stream();
        for i in 0..12 {
            store.put("S", format!("pk-{i}"), vec![0u8; 1_048_576], None);
        }
        let token = iterator_token(&store, "TRIM_HORIZON");
        let (v1, count1, bytes1) = get(&store, &token, None);
        assert_eq!(count1, 10, "10 x 1 MiB == 10 MiB cap; 11th would exceed");
        assert!(bytes1 <= MAX_GET_RECORDS_BYTES as u64);
        let next = v1["NextShardIterator"].as_str().unwrap().to_string();
        let (v2, count2, _) = get(&store, &next, None);
        assert_eq!(count2, 2);
        assert_eq!(v2["Records"][0]["PartitionKey"], "pk-10");
    }

    #[test]
    fn get_records_returns_single_oversize_record_alone() {
        let mut store = store_with_stream();
        store.put("S", "big".into(), vec![0u8; 11 * 1_048_576], None);
        store.put("S", "small".into(), vec![1u8; 4], None);
        let token = iterator_token(&store, "TRIM_HORIZON");
        let (v, count, _) = get(&store, &token, None);
        assert_eq!(count, 1, "oversize record must still make progress");
        assert_eq!(v["Records"][0]["PartitionKey"], "big");
    }

    #[test]
    fn get_records_escapes_partition_key() {
        let mut store = store_with_stream();
        // Includes a raw control char () that expands to the 6-byte \uXXXX
        // form — the worst case for the buffer-capacity estimate.
        let pk = "a\"b\\c\nd\te\u{0001}\u{1f}";
        store.put("S", pk.into(), vec![1, 2, 3], None);
        let token = iterator_token(&store, "TRIM_HORIZON");
        let (v, _, _) = get(&store, &token, None);
        assert_eq!(v["Records"][0]["PartitionKey"], pk);
    }

    proptest! {
        #[test]
        fn get_records_body_matches_reference(
            records in prop::collection::vec(
                (".*", prop::collection::vec(any::<u8>(), 0..40)),
                0..8,
            )
        ) {
            let mut store = store_with_stream();
            for (pk, data) in &records {
                store.put("S", pk.clone(), data.clone(), None);
            }
            let token = iterator_token(&store, "TRIM_HORIZON");
            let (body, count, bytes) =
                get_records(&store, &json!({ "ShardIterator": token })).unwrap();
            let got: Value = serde_json::from_slice(&body).unwrap();
            let recs = got["Records"].as_array().unwrap();
            prop_assert_eq!(recs.len(), records.len());
            prop_assert_eq!(count as usize, records.len());
            let total: usize = records.iter().map(|(_, d)| d.len()).sum();
            prop_assert_eq!(bytes as usize, total);
            for (i, (pk, data)) in records.iter().enumerate() {
                prop_assert_eq!(recs[i]["PartitionKey"].as_str().unwrap(), pk.as_str());
                prop_assert_eq!(recs[i]["Data"].as_str().unwrap(), encode_data(data));
            }
        }
    }
}

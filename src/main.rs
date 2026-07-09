//! fakestream — a minimal, low-memory local Amazon Kinesis Data Streams emulator.
//!
//! Single HTTP endpoint speaking the AWS JSON 1.1 protocol, no async runtime, a
//! small fixed thread pool, and an in-memory store behind one `RwLock`. Point any
//! AWS SDK at `http://localhost:<port>` with dummy credentials and it behaves like
//! Kinesis for the polling-consumer + PutRecord workflows. With `--persist <dir>`
//! state survives restarts: stream definitions live in a manifest, records in a
//! segmented write-ahead log replayed on startup.

mod manifest;
mod metrics;
mod ops;
mod protocol;
mod store;
mod wal;

use std::io::Read;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use serde_json::Value;
use tiny_http::{Header, Method, Request, Response, Server};

use crate::metrics::{b64_decoded_len, Metrics};
use crate::protocol::{parse_target, ApiError};
use crate::store::{now_ms, Store};
use crate::wal::Wal;

const DEFAULT_PORT: u16 = 4567;
const WORKER_THREADS: usize = 4;
const DEFAULT_PERSIST_INTERVAL_SECS: u64 = 5;
const DEFAULT_RETENTION_SECS: u64 = 24 * 3600;
const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
// 16 MiB, comfortably above the 5 MiB PutRecords decoded limit after base64 +
// JSON inflation. Bounds per-request memory against an oversized body.
const MAX_REQUEST_BODY_BYTES: u64 = 16 * 1024 * 1024;

/// A handled operation's result: a small `Value` for most ops, or a pre-built
/// JSON body for GetRecords (serialized in `ops` straight into one buffer to
/// keep the response memory bounded and predictable).
enum OpResponse {
    Json(Value),
    Body(Vec<u8>),
}

/// Runtime configuration assembled from flags and environment variables.
struct Config {
    port: u16,
    persist_interval_secs: u64,
    default_retention_secs: u64,
    persist_dir: Option<String>,
    segment_bytes: u64,
}

fn main() {
    let config = Config::load();
    let addr = format!("0.0.0.0:{}", config.port);

    let server = Arc::new(Server::http(&addr).expect("failed to bind fakestream HTTP server"));
    let (store, wal) = load_state(&config);
    let store = Arc::new(RwLock::new(store));
    let wal = wal.map(|w| Arc::new(Mutex::new(w)));
    let persist_dir = config.persist_dir.clone();
    println!("fakestream listening on http://{addr} (Kinesis, AWS JSON 1.1)");
    println!("  retention: {}s default", config.default_retention_secs);
    match &persist_dir {
        Some(dir) => println!("  persistence: {dir}"),
        None => println!("  persistence: off"),
    }

    spawn_maintenance(&store, &wal, &persist_dir, config.persist_interval_secs);

    let metrics = Arc::new(Metrics::new());

    let mut handles = Vec::with_capacity(WORKER_THREADS);
    for _ in 0..WORKER_THREADS {
        let server = Arc::clone(&server);
        let store = Arc::clone(&store);
        let metrics = Arc::clone(&metrics);
        let wal = wal.clone();
        let persist_dir = persist_dir.clone();
        handles.push(thread::spawn(move || {
            for request in server.incoming_requests() {
                handle(request, &store, &wal, &persist_dir, &metrics);
            }
        }));
    }
    for handle in handles {
        let _ = handle.join();
    }
}

impl Config {
    fn load() -> Self {
        let port = opt("--port", "FAKESTREAM_PORT")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_PORT);
        let persist_interval_secs = opt("--persist-interval", "FAKESTREAM_PERSIST_INTERVAL")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_PERSIST_INTERVAL_SECS)
            .max(1);
        let default_retention_secs = opt("--ttl-seconds", "FAKESTREAM_TTL_SECONDS")
            .and_then(|v| v.parse().ok())
            .or_else(|| {
                opt("--retention-hours", "FAKESTREAM_RETENTION_HOURS")
                    .and_then(|v| v.parse::<u64>().ok())
                    .and_then(|hours| hours.checked_mul(3600))
            })
            .unwrap_or(DEFAULT_RETENTION_SECS);
        let persist_dir = opt("--persist", "FAKESTREAM_PERSIST_PATH").filter(|v| !v.is_empty());
        let segment_bytes = opt("--segment-bytes", "FAKESTREAM_SEGMENT_BYTES")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SEGMENT_BYTES);
        Config {
            port,
            persist_interval_secs,
            default_retention_secs,
            persist_dir,
            segment_bytes,
        }
    }
}

/// Build the store + WAL from disk when `--persist` is set, replaying the log;
/// otherwise an empty in-memory store with no WAL.
fn load_state(config: &Config) -> (Store, Option<Wal>) {
    let Some(dir) = &config.persist_dir else {
        return (Store::new(config.default_retention_secs), None);
    };
    let dir = std::path::Path::new(dir);
    let mut store =
        manifest::load(dir).unwrap_or_else(|| Store::new(config.default_retention_secs));
    store.default_retention_secs = config.default_retention_secs;
    let (wal, entries) = match Wal::load(dir, config.segment_bytes) {
        Ok(pair) => pair,
        Err(err) => {
            eprintln!("fakestream: WAL open failed ({err}); starting without persistence");
            return (store, None);
        }
    };
    // Durability invariant: puts deliberately don't re-save the manifest. The
    // periodic maintenance save (before each segment drop) preserves the seq
    // high-water on disk, and replaying the WAL here recovers any seqs from the
    // active segment that are newer than the last-saved manifest.
    let mut max_seq = 0u64;
    for (stream, shard_id, record) in entries {
        max_seq = max_seq.max(record.seq);
        store.restore_record(&stream, &shard_id, record);
    }
    store.bump_seq_to(max_seq);
    let removed = store.trim_expired(now_ms());
    if removed > 0 {
        println!("  replayed WAL, trimmed {removed} expired record(s)");
    }
    (store, Some(wal))
}

/// Background thread: periodically trim expired records, refresh the manifest,
/// and drop fully-expired WAL segments.
fn spawn_maintenance(
    store: &Arc<RwLock<Store>>,
    wal: &Option<Arc<Mutex<Wal>>>,
    persist_dir: &Option<String>,
    interval_secs: u64,
) {
    let store = Arc::clone(store);
    let wal = wal.clone();
    let persist_dir = persist_dir.clone();
    let interval = Duration::from_secs(interval_secs);
    thread::spawn(move || loop {
        thread::sleep(interval);
        let now = now_ms();
        store
            .write()
            .expect("store lock poisoned")
            .trim_expired(now);
        if let (Some(wal), Some(dir)) = (&wal, &persist_dir) {
            // Persist the manifest (carrying the current seq high-water) BEFORE
            // dropping segments so a crash can't lose it. Lock discipline is
            // store-then-wal: release the store read lock before taking the wal.
            let retentions = {
                let store = store.read().expect("store lock poisoned");
                if let Err(err) = manifest::save(std::path::Path::new(dir), &store) {
                    eprintln!("fakestream: manifest save failed: {err}");
                }
                store.stream_retentions()
            };
            if let Err(err) = wal
                .lock()
                .expect("wal lock poisoned")
                .drop_expired(now, &retentions)
            {
                eprintln!("fakestream: segment drop failed: {err}");
            }
        }
    });
}

/// Read a value from an env var, falling back to a CLI flag.
fn opt(flag: &str, env: &str) -> Option<String> {
    std::env::var(env).ok().or_else(|| arg_value(flag))
}

fn arg_value(flag: &str) -> Option<String> {
    let mut args = std::env::args();
    while let Some(arg) = args.next() {
        if arg == flag {
            return args.next();
        }
    }
    None
}

fn handle(
    mut request: Request,
    store: &Arc<RwLock<Store>>,
    wal: &Option<Arc<Mutex<Wal>>>,
    persist_dir: &Option<String>,
    metrics: &Arc<Metrics>,
) {
    if request.method() == &Method::Get {
        if request.url() == "/metrics" {
            let body = metrics.render(&read(store));
            let header =
                Header::from_bytes(&b"Content-Type"[..], &b"text/plain; version=0.0.4"[..])
                    .expect("static header is valid");
            let _ = request.respond(Response::from_string(body).with_header(header));
        } else {
            let _ = request.respond(Response::from_string("fakestream ok"));
        }
        return;
    }
    let target = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("X-Amz-Target"))
        .map(|h| h.value.as_str().to_string());

    let mut body = String::new();
    // Read one byte past the cap with `take` so an oversized body is detected
    // without ever buffering more than the limit.
    let mut reader = request.as_reader().take(MAX_REQUEST_BODY_BYTES + 1);
    if reader.read_to_string(&mut body).is_err() {
        respond_error(
            request,
            &ApiError::new("SerializationException", "Unreadable request body"),
        );
        return;
    }
    if body.len() as u64 > MAX_REQUEST_BODY_BYTES {
        respond_error(
            request,
            &ApiError::validation("Request body exceeds the 16 MiB limit"),
        );
        return;
    }

    match route(target.as_deref(), &body, store, wal, persist_dir, metrics) {
        Ok(OpResponse::Json(json)) => respond_ok_json(request, &json),
        Ok(OpResponse::Body(body)) => respond_ok_body(request, body),
        Err(err) => respond_error(request, &err),
    }
}

fn route(
    target: Option<&str>,
    body: &str,
    store: &Arc<RwLock<Store>>,
    wal: &Option<Arc<Mutex<Wal>>>,
    persist_dir: &Option<String>,
    metrics: &Arc<Metrics>,
) -> Result<OpResponse, ApiError> {
    let op = target.and_then(parse_target).ok_or_else(|| {
        ApiError::new(
            "UnknownOperationException",
            "Missing or invalid X-Amz-Target",
        )
    })?;
    let req: Value = if body.trim().is_empty() {
        Value::Object(Default::default())
    } else {
        serde_json::from_str(body).map_err(|_| {
            ApiError::new("SerializationException", "Request body is not valid JSON")
        })?
    };

    metrics.record_request(op);

    // GetRecords serializes its own (bounded) body and reports exact counts, so
    // it bypasses the Value-returning path and its byte-counting re-walk.
    if op == "GetRecords" {
        let (body, count, bytes) = ops::get_records(&read(store), &req)?;
        metrics.add_get(count, bytes);
        return Ok(OpResponse::Body(body));
    }

    let result = match op {
        "CreateStream" => {
            let mut s = write(store);
            let result = ops::create_stream(&mut s, &req);
            if result.is_ok() {
                save_manifest(persist_dir, &s);
            }
            result
        }
        "DeleteStream" => {
            let mut s = write(store);
            let result = ops::delete_stream(&mut s, &req);
            if result.is_ok() {
                save_manifest(persist_dir, &s);
            }
            result
        }
        "PutRecord" => with_wal(store, wal, |s, w| ops::put_record(s, w, &req)),
        "PutRecords" => with_wal(store, wal, |s, w| ops::put_records(s, w, &req)),
        "ListStreams" => ops::list_streams(&read(store), &req),
        "DescribeStream" => ops::describe_stream(&read(store), &req),
        "DescribeStreamSummary" => ops::describe_stream_summary(&read(store), &req),
        "ListShards" => ops::list_shards(&read(store), &req),
        "GetShardIterator" => ops::get_shard_iterator(&read(store), &req),
        other => Err(ApiError::new(
            "UnknownOperationException",
            format!("Operation {other} is not implemented by fakestream"),
        )),
    };

    if let Ok(response) = &result {
        match op {
            "PutRecord" => {
                let bytes = req
                    .get("Data")
                    .and_then(Value::as_str)
                    .map(b64_decoded_len)
                    .unwrap_or(0);
                metrics.add_put(1, bytes);
            }
            "PutRecords" => {
                if let (Some(request_records), Some(response_records)) = (
                    req.get("Records").and_then(Value::as_array),
                    response.get("Records").and_then(Value::as_array),
                ) {
                    let (records, bytes) =
                        successful_put_metrics(request_records, response_records);
                    metrics.add_put(records, bytes);
                }
            }
            _ => {}
        }
    }

    result.map(OpResponse::Json)
}

/// Count records and decoded bytes for the PutRecords entries that succeeded,
/// correlating each request record with its same-index response entry. Entries
/// whose response carries an `ErrorCode` failed routing and are excluded so
/// metrics never over-count a partially-failed batch.
fn successful_put_metrics(request_records: &[Value], response_records: &[Value]) -> (u64, u64) {
    let mut records = 0u64;
    let mut bytes = 0u64;
    for (request, result) in request_records.iter().zip(response_records) {
        if result.get("ErrorCode").is_some() {
            continue;
        }
        records += 1;
        bytes += request
            .get("Data")
            .and_then(Value::as_str)
            .map(b64_decoded_len)
            .unwrap_or(0);
    }
    (records, bytes)
}

/// Run a write op under the store write lock, with the WAL locked alongside it
/// (or `None` when persistence is off).
fn with_wal(
    store: &Arc<RwLock<Store>>,
    wal: &Option<Arc<Mutex<Wal>>>,
    f: impl FnOnce(&mut Store, Option<&mut Wal>) -> Result<Value, ApiError>,
) -> Result<Value, ApiError> {
    let mut store = store.write().expect("store lock poisoned");
    match wal {
        Some(w) => {
            let mut wal = w.lock().expect("wal lock poisoned");
            f(&mut store, Some(&mut wal))
        }
        None => f(&mut store, None),
    }
}

/// Rewrite the stream-definition manifest when persistence is on.
fn save_manifest(persist_dir: &Option<String>, store: &Store) {
    if let Some(dir) = persist_dir {
        if let Err(err) = manifest::save(std::path::Path::new(dir), store) {
            eprintln!("fakestream: manifest save failed: {err}");
        }
    }
}

fn write(store: &Arc<RwLock<Store>>) -> std::sync::RwLockWriteGuard<'_, Store> {
    store.write().expect("store lock poisoned")
}

fn read(store: &Arc<RwLock<Store>>) -> std::sync::RwLockReadGuard<'_, Store> {
    store.read().expect("store lock poisoned")
}

fn json_header() -> Header {
    Header::from_bytes(&b"Content-Type"[..], &b"application/x-amz-json-1.1"[..])
        .expect("static header is valid")
}

fn respond_ok_json(request: Request, json: &Value) {
    let response = Response::from_string(json.to_string()).with_header(json_header());
    let _ = request.respond(response);
}

fn respond_ok_body(request: Request, body: Vec<u8>) {
    let response = Response::from_data(body).with_header(json_header());
    let _ = request.respond(response);
}

fn respond_error(request: Request, err: &ApiError) {
    let error_type = Header::from_bytes(&b"x-amzn-errortype"[..], err.kind.as_bytes())
        .expect("error kind is valid header value");
    let response = Response::from_string(err.body())
        .with_status_code(err.status)
        .with_header(json_header())
        .with_header(error_type);
    let _ = request.respond(response);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn successful_put_metrics_excludes_failed_entries() {
        let request = [json!({ "Data": "AAAA" }), json!({ "Data": "AAAA" })];
        let response = [
            json!({ "ShardId": "shardId-000000000000", "SequenceNumber": "1" }),
            json!({ "ErrorCode": "InternalFailure", "ErrorMessage": "boom" }),
        ];
        let (records, bytes) = successful_put_metrics(&request, &response);
        assert_eq!(records, 1);
        assert_eq!(bytes, 3); // one "AAAA" decodes to 3 bytes
    }

    #[test]
    fn successful_put_metrics_counts_all_when_none_failed() {
        let request = [json!({ "Data": "AAAA" }), json!({ "Data": "AAA=" })];
        let response = [
            json!({ "ShardId": "shardId-000000000000", "SequenceNumber": "1" }),
            json!({ "ShardId": "shardId-000000000000", "SequenceNumber": "2" }),
        ];
        let (records, bytes) = successful_put_metrics(&request, &response);
        assert_eq!(records, 2);
        assert_eq!(bytes, 5); // 3 + 2
    }
}

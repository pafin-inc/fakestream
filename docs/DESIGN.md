# fakestream — design

A minimal, low-memory local Amazon Kinesis Data Streams emulator in Rust. Built to be a
**drop-in replacement for LocalStack's Kinesis** for local development — and nothing else
(DynamoDB etc. stay where they are).

## Goals / non-goals

**Goals**
- Speak enough of the Kinesis wire protocol that the AWS SDKs (`boto3`, `aws-sdk-js v3`) and `aws-cli` work unchanged — just point them at `http://localhost:<port>`.
- Tiny memory footprint and instant startup. No JVM, no Python, no async runtime.
- Single static-ish binary or small Docker image.
- Correct-enough semantics for **polling consumers**: ordered sequence numbers, shard iterators with expiry, `TRIM_HORIZON` / `AFTER_SEQUENCE_NUMBER`, `ResourceNotFoundException`, `ExpiredIteratorException`.

**Non-goals (deliberately not built)**
- Enhanced fan-out / `SubscribeToShard` (HTTP/2 push) — the consumers poll.
- Resharding (`SplitShard`/`MergeShards`/`UpdateShardCount`) — consumer assumes one shard.
- Encryption, tagging, resource policies, account settings, KCL lease coordination.
- Auth/SigV4 verification — accepts any credentials, like every local mock.

Scope is set by what the code actually calls (see `RESEARCH.md`), not by the full API.

## Wire protocol

Kinesis is a single endpoint: `POST /` with

- `X-Amz-Target: Kinesis_20131202.<Operation>`
- `Content-Type: application/x-amz-json-1.1`
- JSON body; record payloads (`Data`) are **base64** in JSON.

Success → HTTP 200 + JSON. Error → HTTP 400 (500 for `InternalFailure`) +
`{"__type":"<Exception>","message":"…"}` and an
`x-amzn-errortype` header. Both `boto3` and `aws-sdk-js v3` read `__type` to build the typed error
(this is why `ExpiredIteratorException` correctly triggers the consumer's iterator-restart path).

## Architecture

No async runtime. A fixed pool of 4 OS threads share one `Arc<RwLock<Store>>` and pull from
`tiny_http`'s request queue. Reads (`GetRecords`, `DescribeStream`, …) take a read lock; mutations
(`PutRecord(s)`, `CreateStream`, `DeleteStream`) take a write lock. For a one-shard, ~1 req/s local
consumer this is far more than enough and keeps idle memory near zero. One extra background thread
(the maintenance thread) periodically trims expired records and, if persistence is on, snapshots.

```
main.rs        config (flags/env), HTTP loop, op routing, maintenance thread, response/error encoding
protocol.rs    X-Amz-Target parsing, ApiError → AWS JSON error body, base64 helpers
store.rs       Store/Stream/Shard/Record, MD5 hash-key routing, sequence counter, iterators, trim
manifest.rs    atomic JSON manifest save + load-on-startup (stream defs + seq high-water)
wal.rs         segmented append-only WAL: length-framed postcard records, segment roll + drop
ops.rs         one handler per operation; request parsing + JSON response shaping
```

Dependencies (intentionally few): `tiny_http` (blocking HTTP, no tokio), `serde`/`serde_json`,
`postcard`, `base64`, `md-5`. Release profile is size-optimized (`opt-level="z"`, LTO, `panic="abort"`, stripped).

## Persistence & retention

Opt-in via `--persist <dir>` (default: pure in-memory). The value is a **directory**. On startup,
`manifest.json` inside that directory is loaded to recover stream definitions and the sequence
counter high-water, then all WAL segments under `<dir>/wal/` are replayed in order to recover
records. After replay, expired records are trimmed and the store is ready.

Two files make up the persistent state:

- **`manifest.json`** — stream definitions plus the global sequence counter. Atomically rewritten
  (write to `<dir>/manifest.json.tmp`, then rename) whenever a stream is created or deleted, and on
  each maintenance tick. Records are **not** stored here; a pure-manifest restart with no WAL
  recovers stream definitions only.
- **`<dir>/wal/seg-NNNNNNNNNN.log`** — length-framed postcard records (8-byte LE length prefix +
  postcard body) in an append-only segmented file. Each new `PutRecord`/`PutRecords` call appends
  frames to the active segment. When the active segment reaches `--segment-bytes` (default 64 MiB),
  a new segment is opened. On replay, a crash-torn trailing frame (length prefix present but body
  truncated) is detected and the file is truncated to the last good byte so future appends stay
  clean.

The maintenance thread runs every `--persist-interval` seconds (default 5). It trims expired
records in memory, saves the manifest (preserving the current seq high-water), then drops any
closed WAL segments whose newest record is older than the stream's retention. Segment drops happen
**after** the manifest save, so a crash can't lose the seq high-water. The only loss window is
records appended since the last maintenance tick.

`--segment-bytes` / `FAKESTREAM_SEGMENT_BYTES` controls the segment roll size (default 64 MiB).
Smaller values mean more segments and more frequent drop-eligible boundaries; larger values reduce
file-open overhead.

Why this is enough for "keep unconsumed records": the consumers don't rely on server-side
consumption state — they checkpoint in DynamoDB and resume with `AFTER_SEQUENCE_NUMBER`. So as long
as records and the sequence counter survive the restart, already-consumed records are skipped (the
checkpoint is past them) and unconsumed ones are re-read. Persisting the counter is essential:
without it, restarts would reissue old sequence numbers and the consumer would mis-skip new records.

Retention/TTL is the Kinesis-native `RetentionPeriodHours` per stream, defaulting to the configured
global value (`--retention-hours`, or the precise `--ttl-seconds`). `trim_expired` drops records
older than their stream's retention on each maintenance tick and on load, so expired records are
neither served nor replayed from the WAL. `--ttl-seconds 0` disables trimming (keep forever).

## Data model

- **Sequence numbers:** one global monotonic `u64`, formatted as a decimal string. Clients treat
  sequence numbers as opaque and compare via the API, so this is wire-compatible and trivially
  ordered. Checkpoints stored by the consumer in DynamoDB round-trip back through
  `AFTER_SEQUENCE_NUMBER`.
- **Shards:** the 128-bit MD5 partition-key ring split into N contiguous, equal slices. `PutRecord`
  hashes `PartitionKey` with MD5 (big-endian `u128`), or uses `ExplicitHashKey` if given, and routes
  to the owning shard — the same scheme real Kinesis uses, so multi-shard streams distribute
  correctly.
- **Records:** `Vec<Record>` per shard (`seq`, `partition_key`, `data: Vec<u8>`, `timestamp_ms`),
  appended in order. Memory is bounded by retained records only.
- **Shard iterators:** opaque base64(JSON) tokens carrying `{stream, shard_id, next_seq, expires_ms}`,
  with a 5-minute TTL. `GetRecords` returns records with `seq >= next_seq` up to `Limit`, advances
  `next_seq`, and re-mints the token; an expired token yields `ExpiredIteratorException`.
  `MillisBehindLatest` is 0 when caught up (consumer polls every 1s) and the lag in ms otherwise
  (consumer polls every 250ms).

## Implemented operations

`CreateStream`, `DeleteStream`, `ListStreams`, `DescribeStream`, `DescribeStreamSummary`,
`ListShards`, `GetShardIterator`, `PutRecord`, `PutRecords`, `GetRecords`.

(`PutRecords`, `DeleteStream`, `DescribeStreamSummary` aren't strictly used today but are cheap and
commonly expected.)

## Verified behavior (aws-cli + curl)

- `create-stream` (2 shards) → `describe-stream`/`list-shards` show correct contiguous 128-bit hash ranges.
- `put-record` ×3 → `get-records` returns them in order; base64 payloads decode exactly.
- `AFTER_SEQUENCE_NUMBER 1` returns seq 2,3 (checkpoint-resume path).
- `NextShardIterator` advances; a follow-up poll returns empty with `MillisBehindLatest: 0`.
- Unknown stream → `ResourceNotFoundException` (parsed by aws-cli; raw `__type` body confirmed).
- `PutRecords` batch of 500 → `FailedRecordCount: 0`; `GetRecords --limit 10000` honored.
- **Footprint:** idle RSS ~2.2 MB; ~11.8 MB holding ~20k records of 200 B; 493 KB release binary.

## Possible next steps (post-MVP)

- ~~Optional disk persistence~~ — done (`--persist <dir>`, manifest + segmented WAL).
- ~~Retention trimming~~ — done (`--retention-hours` / `--ttl-seconds`, `trim_expired`).
- A tiny read-only web UI to inspect streams/shards/records (in the original pitch).
- `SplitShard`/`MergeShards` if we ever want to demo resharding correctness — the differentiator
  none of the alternatives handle well.

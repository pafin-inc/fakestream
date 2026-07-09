//! In-memory Kinesis data model: streams, shards, records, and iterator tokens.
//!
//! Everything lives behind a single `RwLock<Store>` in `main`. Records are kept
//! as `Vec<u8>` payloads ordered by a global monotonic sequence number, which is
//! all that polling consumers rely on. Stream and shard
//! metadata is `Serialize`/`Deserialize` for the manifest JSON; record payloads
//! are skipped there (`#[serde(skip)]` on `Shard::records`) and persisted
//! separately via the WAL (postcard, raw bytes — no base64 inflation).

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

const ITERATOR_TTL_MS: u128 = 300_000; // AWS shard iterators expire after 5 minutes.

/// A single record stored in a shard. Persisted to the WAL via postcard (raw
/// bytes — no base64 inflation); skipped in the manifest JSON.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub partition_key: String,
    pub data: Vec<u8>,
    pub timestamp_ms: u128,
}

/// One shard: a contiguous slice of the 128-bit partition-key hash space.
#[derive(Serialize, Deserialize)]
pub struct Shard {
    pub id: String,
    pub hash_start: u128,
    pub hash_end: u128,
    #[serde(skip)]
    pub records: Vec<Record>,
    pub closed: bool,
}

/// A stream and its shards.
#[derive(Serialize, Deserialize)]
pub struct Stream {
    pub name: String,
    pub arn: String,
    pub shards: Vec<Shard>,
    pub retention_secs: u64,
    pub created_ms: u128,
}

/// Top-level state: all streams, the global sequence counter, and the default
/// retention applied to streams created without an explicit `RetentionPeriodHours`.
#[derive(Serialize, Deserialize)]
pub struct Store {
    pub streams: HashMap<String, Stream>,
    seq_counter: u64,
    pub default_retention_secs: u64,
}

/// Opaque shard-iterator payload, serialized to base64 JSON for the client.
#[derive(Serialize, Deserialize)]
pub struct Iterator {
    pub stream: String,
    pub shard_id: String,
    pub next_seq: u64,
    pub expires_ms: u128,
}

pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Hash a partition key into the 128-bit ring the way Kinesis does (MD5, big-endian).
pub fn hash_key(partition_key: &str) -> u128 {
    use md5::{Digest, Md5};
    let digest = Md5::digest(partition_key.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest);
    u128::from_be_bytes(bytes)
}

fn shard_id(index: u32) -> String {
    format!("shardId-{index:012}")
}

impl Store {
    pub fn new(default_retention_secs: u64) -> Self {
        Store {
            streams: HashMap::new(),
            seq_counter: 0,
            default_retention_secs,
        }
    }

    fn next_seq(&mut self) -> u64 {
        self.seq_counter += 1;
        self.seq_counter
    }

    pub fn create_stream(
        &mut self,
        name: &str,
        shard_count: u32,
        retention_secs: Option<u64>,
    ) -> bool {
        if self.streams.contains_key(name) {
            return false;
        }
        let count = shard_count.max(1);
        let step = u128::MAX / count as u128;
        let mut shards = Vec::with_capacity(count as usize);
        for i in 0..count {
            let hash_start = i as u128 * step;
            let hash_end = if i == count - 1 {
                u128::MAX
            } else {
                (i as u128 + 1) * step - 1
            };
            shards.push(Shard {
                id: shard_id(i),
                hash_start,
                hash_end,
                records: Vec::new(),
                closed: false,
            });
        }
        let arn = format!("arn:aws:kinesis:us-east-1:000000000000:stream/{name}");
        let retention_secs = retention_secs.unwrap_or(self.default_retention_secs);
        self.streams.insert(
            name.to_string(),
            Stream {
                name: name.to_string(),
                arn,
                shards,
                retention_secs,
                created_ms: now_ms(),
            },
        );
        true
    }

    /// Drop records older than their stream's retention. Returns how many were removed.
    /// A `retention_secs` of 0 disables trimming (records kept forever).
    pub fn trim_expired(&mut self, now: u128) -> usize {
        let mut removed = 0;
        for stream in self.streams.values_mut() {
            if stream.retention_secs == 0 {
                continue;
            }
            let cutoff_ms = (stream.retention_secs as u128) * 1000;
            for shard in &mut stream.shards {
                let before = shard.records.len();
                shard
                    .records
                    .retain(|r| now.saturating_sub(r.timestamp_ms) <= cutoff_ms);
                removed += before - shard.records.len();
            }
        }
        removed
    }

    /// Route a record to its shard by hash key and append it. Returns (shard_id, seq).
    pub fn put(
        &mut self,
        stream: &str,
        partition_key: String,
        data: Vec<u8>,
        explicit_hash_key: Option<u128>,
    ) -> Option<(String, u64)> {
        let seq = self.next_seq();
        let now = now_ms();
        let hash = explicit_hash_key.unwrap_or_else(|| hash_key(&partition_key));
        let stream = self.streams.get_mut(stream)?;
        let shard = stream
            .shards
            .iter_mut()
            .find(|s| !s.closed && hash >= s.hash_start && hash <= s.hash_end)?;
        let shard_id = shard.id.clone();
        shard.records.push(Record {
            seq,
            partition_key,
            data,
            timestamp_ms: now,
        });
        Some((shard_id, seq))
    }

    /// (stream name, record count, total payload bytes) for every stream.
    /// Used by the metrics endpoint to expose per-stream gauges without
    /// exposing internal fields.
    pub fn stream_sizes(&self) -> Vec<(String, u64, u64)> {
        let mut out = Vec::with_capacity(self.streams.len());
        for (name, stream) in &self.streams {
            let mut records = 0u64;
            let mut bytes = 0u64;
            for shard in &stream.shards {
                records += shard.records.len() as u64;
                for record in &shard.records {
                    bytes += record.data.len() as u64;
                }
            }
            out.push((name.clone(), records, bytes));
        }
        out
    }

    pub fn make_iterator(
        &self,
        stream: &str,
        shard_id: &str,
        iterator_type: &str,
        starting_seq: Option<u64>,
        timestamp_ms: Option<u128>,
    ) -> Option<Iterator> {
        let shard = self
            .streams
            .get(stream)?
            .shards
            .iter()
            .find(|s| s.id == shard_id)?;
        let next_seq = resolve_start(shard, iterator_type, starting_seq, timestamp_ms)?;
        Some(Iterator {
            stream: stream.to_string(),
            shard_id: shard_id.to_string(),
            next_seq,
            expires_ms: now_ms() + ITERATOR_TTL_MS,
        })
    }
}

/// WAL-replay API: insert records preserving their seqs, raise the seq floor,
/// and inspect retention/high-water for the persistence loop.
impl Store {
    /// Insert a record preserving its existing seq (used by WAL replay). Does
    /// not advance the seq counter. Returns false if the stream/shard is unknown.
    pub fn restore_record(&mut self, stream: &str, shard_id: &str, record: Record) -> bool {
        let Some(stream) = self.streams.get_mut(stream) else {
            return false;
        };
        // Drop records written before this stream was created. Shard IDs are
        // deterministic, so a delete-then-recreate of the same name reuses them;
        // without this check a deleted stream's WAL records would resurrect in
        // the recreated stream on replay. A record written in the same
        // millisecond as creation still belongs to it, so the compare is strict.
        if record.timestamp_ms < stream.created_ms {
            return false;
        }
        let Some(shard) = stream.shards.iter_mut().find(|s| s.id == shard_id) else {
            return false;
        };
        shard.records.push(record);
        true
    }

    /// Raise the seq counter floor so freshly-assigned seqs never collide with
    /// replayed ones.
    pub fn bump_seq_to(&mut self, seq: u64) {
        if seq > self.seq_counter {
            self.seq_counter = seq;
        }
    }

    /// The last (highest-seq) record appended to a shard, if any.
    pub fn last_record(&self, stream: &str, shard_id: &str) -> Option<&Record> {
        self.streams
            .get(stream)?
            .shards
            .iter()
            .find(|s| s.id == shard_id)?
            .records
            .last()
    }

    /// Largest retention across all current streams (0 = keep-forever wins).
    pub fn max_retention_secs(&self) -> u64 {
        if self.streams.values().any(|s| s.retention_secs == 0) {
            return 0;
        }
        self.streams
            .values()
            .map(|s| s.retention_secs)
            .max()
            .unwrap_or(self.default_retention_secs)
    }
}

/// Translate a Kinesis iterator type into the sequence number to start reading from.
fn resolve_start(
    shard: &Shard,
    iterator_type: &str,
    starting_seq: Option<u64>,
    timestamp_ms: Option<u128>,
) -> Option<u64> {
    let latest = shard.records.last().map(|r| r.seq).unwrap_or(0);
    match iterator_type {
        "TRIM_HORIZON" => Some(0),
        "LATEST" => Some(latest + 1),
        "AT_SEQUENCE_NUMBER" => starting_seq,
        "AFTER_SEQUENCE_NUMBER" => starting_seq.map(|s| s + 1),
        "AT_TIMESTAMP" => {
            let ts = timestamp_ms?;
            let first = shard
                .records
                .iter()
                .find(|r| r.timestamp_ms >= ts)
                .map(|r| r.seq);
            Some(first.unwrap_or(latest + 1))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(seq: u64) -> Record {
        Record {
            seq,
            partition_key: "p".into(),
            data: vec![1, 2, 3],
            timestamp_ms: 100,
        }
    }

    #[test]
    fn restore_record_preserves_seq_and_does_not_increment_counter() {
        let mut s = Store::new(86_400);
        s.create_stream("S", 1, None);
        // rec()'s timestamp predates a real creation clock; float creation back
        // so replay accepts it (the resurrection guard uses created_ms).
        s.streams.get_mut("S").unwrap().created_ms = 0;
        assert!(s.restore_record("S", "shardId-000000000000", rec(42)));
        assert_eq!(s.last_record("S", "shardId-000000000000").unwrap().seq, 42);
        s.bump_seq_to(42);
        let (_, seq) = s.put("S", "p".into(), vec![9], None).unwrap();
        assert_eq!(seq, 43);
    }

    #[test]
    fn manifest_serde_round_trip_drops_records() {
        let mut s = Store::new(86_400);
        s.create_stream("S", 1, None);
        s.restore_record("S", "shardId-000000000000", rec(1));
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("\"records\""));
        let back: Store = serde_json::from_str(&json).unwrap();
        assert!(back.streams.contains_key("S"));
        assert_eq!(back.last_record("S", "shardId-000000000000"), None);
    }

    #[test]
    fn max_retention_is_the_largest_stream() {
        let mut s = Store::new(3600);
        s.create_stream("A", 1, Some(3600));
        s.create_stream("B", 1, Some(86_400));
        assert_eq!(s.max_retention_secs(), 86_400);
    }
}

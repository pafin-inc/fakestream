//! Prometheus metrics for fakestream: lock-free cumulative counters plus
//! gauges computed from the live store at scrape time.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::store::Store;

/// Every Kinesis operation fakestream implements. Pre-populated into the
/// request-counter map so increments never need a lock.
pub const OPS: [&str; 10] = [
    "CreateStream",
    "DeleteStream",
    "PutRecord",
    "PutRecords",
    "ListStreams",
    "DescribeStream",
    "DescribeStreamSummary",
    "ListShards",
    "GetShardIterator",
    "GetRecords",
];

pub struct Metrics {
    put_records: AtomicU64,
    get_records: AtomicU64,
    put_bytes: AtomicU64,
    get_bytes: AtomicU64,
    requests: HashMap<&'static str, AtomicU64>,
    start_secs: u64,
}

impl Metrics {
    pub fn new() -> Self {
        let mut requests = HashMap::new();
        for op in OPS {
            requests.insert(op, AtomicU64::new(0));
        }
        let start_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        Metrics {
            put_records: AtomicU64::new(0),
            get_records: AtomicU64::new(0),
            put_bytes: AtomicU64::new(0),
            get_bytes: AtomicU64::new(0),
            requests,
            start_secs,
        }
    }

    pub fn record_request(&self, op: &str) {
        if let Some(counter) = self.requests.get(op) {
            counter.fetch_add(1, Relaxed);
        }
    }

    pub fn add_put(&self, records: u64, bytes: u64) {
        self.put_records.fetch_add(records, Relaxed);
        self.put_bytes.fetch_add(bytes, Relaxed);
    }

    pub fn add_get(&self, records: u64, bytes: u64) {
        self.get_records.fetch_add(records, Relaxed);
        self.get_bytes.fetch_add(bytes, Relaxed);
    }

    pub fn render(&self, store: &Store) -> String {
        let mut out = String::with_capacity(1024);
        out.push_str("# TYPE fakestream_put_records_total counter\n");
        out.push_str(&format!(
            "fakestream_put_records_total {}\n",
            self.put_records.load(Relaxed)
        ));
        out.push_str("# TYPE fakestream_get_records_total counter\n");
        out.push_str(&format!(
            "fakestream_get_records_total {}\n",
            self.get_records.load(Relaxed)
        ));
        out.push_str("# TYPE fakestream_put_bytes_total counter\n");
        out.push_str(&format!(
            "fakestream_put_bytes_total {}\n",
            self.put_bytes.load(Relaxed)
        ));
        out.push_str("# TYPE fakestream_get_bytes_total counter\n");
        out.push_str(&format!(
            "fakestream_get_bytes_total {}\n",
            self.get_bytes.load(Relaxed)
        ));
        out.push_str("# TYPE fakestream_requests_total counter\n");
        for op in OPS {
            let value = self.requests.get(op).map_or(0, |c| c.load(Relaxed));
            out.push_str(&format!(
                "fakestream_requests_total{{op=\"{op}\"}} {value}\n"
            ));
        }
        out.push_str("# TYPE fakestream_records_stored gauge\n");
        out.push_str("# TYPE fakestream_bytes_stored gauge\n");
        for (name, records, bytes) in store.stream_sizes() {
            out.push_str(&format!(
                "fakestream_records_stored{{stream=\"{name}\"}} {records}\n"
            ));
            out.push_str(&format!(
                "fakestream_bytes_stored{{stream=\"{name}\"}} {bytes}\n"
            ));
        }
        out.push_str("# TYPE process_start_time_seconds gauge\n");
        out.push_str(&format!("process_start_time_seconds {}\n", self.start_secs));
        #[cfg(target_os = "linux")]
        if let Some(rss) = resident_memory_bytes() {
            out.push_str("# TYPE process_resident_memory_bytes gauge\n");
            out.push_str(&format!("process_resident_memory_bytes {rss}\n"));
        }
        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Decoded length of a standard (padded) base64 string, computed without
/// allocating — used to count payload bytes from the wire form.
pub fn b64_decoded_len(s: &str) -> u64 {
    let len = s.len();
    if len == 0 {
        return 0;
    }
    let padding = s.bytes().rev().take_while(|&b| b == b'=').count();
    ((len / 4) * 3).saturating_sub(padding) as u64
}

#[cfg(target_os = "linux")]
fn resident_memory_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(resident_pages * 4096)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[test]
    fn counters_render_after_increment() {
        let metrics = Metrics::new();
        metrics.record_request("PutRecord");
        metrics.add_put(2, 100);
        metrics.add_get(1, 50);
        let text = metrics.render(&Store::new(0));
        assert!(text.contains("fakestream_put_records_total 2"), "{text}");
        assert!(text.contains("fakestream_put_bytes_total 100"), "{text}");
        assert!(text.contains("fakestream_get_records_total 1"), "{text}");
        assert!(text.contains("fakestream_get_bytes_total 50"), "{text}");
        assert!(
            text.contains("fakestream_requests_total{op=\"PutRecord\"} 1"),
            "{text}"
        );
    }

    #[test]
    fn gauges_reflect_store_contents() {
        let mut store = Store::new(0);
        store.create_stream("API-TRANSACTIONS", 1, None);
        // put(stream, partition_key: String, data: Vec<u8>, explicit_hash_key: Option<u128>)
        store.put("API-TRANSACTIONS", "pk".to_string(), vec![1, 2, 3, 4], None);
        let text = Metrics::new().render(&store);
        assert!(
            text.contains("fakestream_records_stored{stream=\"API-TRANSACTIONS\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("fakestream_bytes_stored{stream=\"API-TRANSACTIONS\"} 4"),
            "{text}"
        );
    }

    #[test]
    fn b64_len_matches_decoded_size() {
        assert_eq!(b64_decoded_len("AAAA"), 3);
        assert_eq!(b64_decoded_len("AAA="), 2);
        assert_eq!(b64_decoded_len("AA=="), 1);
        assert_eq!(b64_decoded_len(""), 0);
    }
}

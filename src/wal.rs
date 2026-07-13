//! Append-only, segmented write-ahead log for records. Each frame is
//! `[u64 length LE][postcard((stream, shard_id, record))]`. The length prefix
//! makes a crash-truncated trailing frame detectable on replay. Segments are
//! dropped whole once every record in them is past retention, so there is never
//! a full-store serialization.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::store::Record;

const SUBDIR: &str = "wal";

/// One decoded log entry: which stream/shard a record belongs to, plus the record.
pub type Entry = (String, String, Record);

/// Encode one framed entry: 8-byte LE length prefix + postcard body. Borrows the
/// record (no payload clone).
#[expect(
    clippy::expect_used,
    reason = "Record's concrete Serialize implementation cannot reject postcard encoding"
)]
pub fn encode_frame(stream: &str, shard_id: &str, record: &Record) -> Vec<u8> {
    #[derive(Serialize)]
    struct FrameRef<'a> {
        s: &'a str,
        sh: &'a str,
        r: &'a Record,
    }
    let body = postcard::to_allocvec(&FrameRef {
        s: stream,
        sh: shard_id,
        r: record,
    })
    .expect("postcard encode of a record cannot fail");
    let mut frame = Vec::with_capacity(8 + body.len());
    frame.extend_from_slice(&(body.len() as u64).to_le_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// Decode all complete frames in a segment's bytes. Returns the entries plus the
/// byte offset of the first incomplete/corrupt frame (the safe truncation point);
/// for a clean segment this equals `bytes.len()`.
pub fn decode_segment(bytes: &[u8]) -> (Vec<Entry>, usize) {
    let mut out = Vec::new();
    let mut off = 0usize;
    while off + 8 <= bytes.len() {
        let mut length_bytes = [0u8; 8];
        length_bytes.copy_from_slice(&bytes[off..off + 8]);
        let len = u64::from_le_bytes(length_bytes) as usize;
        let body_start = off + 8;
        let body_end = match body_start.checked_add(len) {
            Some(end) if end <= bytes.len() => end,
            _ => break, // torn: length runs past EOF
        };
        match postcard::from_bytes::<Entry>(&bytes[body_start..body_end]) {
            Ok(entry) => {
                out.push(entry);
                off = body_end;
            }
            Err(_) => break, // corrupt tail
        }
    }
    (out, off)
}

/// Bookkeeping for one closed segment, used to decide when it can be dropped.
/// `max_ts` is per-stream: a segment can only be dropped once every stream it
/// holds records for agrees, so one keep-forever stream can't pin segments that
/// otherwise hold only another stream's long-expired records.
struct Segment {
    path: PathBuf,
    max_ts: HashMap<String, u128>,
}

/// Append-only segmented log writer.
pub struct Wal {
    dir: PathBuf, // <persist_dir>/wal
    segment_max: u64,
    closed: Vec<Segment>, // older, full segments (drop candidates)
    active_id: u64,
    active: Option<BufWriter<File>>,
    active_bytes: u64,
    active_max_ts: HashMap<String, u128>,
    poisoned: bool, // a failed append may have torn the active segment; roll before the next
}

fn seg_name(id: u64) -> String {
    format!("seg-{id:010}.log")
}

impl Wal {
    /// Open (or create) the WAL under `<dir>/wal`, replaying every segment to
    /// rebuild state. Returns the writer (ready to append) plus all replayed
    /// entries in append order. Truncates a crash-torn trailing frame.
    pub fn load(dir: &Path, segment_max: u64) -> io::Result<(Self, Vec<Entry>)> {
        let wal_dir = dir.join(SUBDIR);
        fs::create_dir_all(&wal_dir)?;

        let mut ids: Vec<u64> = fs::read_dir(&wal_dir)?
            .filter_map(Result::ok)
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                name.strip_prefix("seg-")?
                    .strip_suffix(".log")?
                    .parse::<u64>()
                    .ok()
            })
            .collect();
        ids.sort_unstable();

        let mut entries = Vec::new();
        let mut closed = Vec::new();
        let mut active_max_ts = HashMap::new();
        for (idx, &id) in ids.iter().enumerate() {
            let path = wal_dir.join(seg_name(id));
            let bytes = fs::read(&path)?;
            let (mut seg_entries, good_off) = decode_segment(&bytes);
            let is_last = idx + 1 == ids.len();
            if good_off < bytes.len() {
                if is_last {
                    // Crash-torn trailing frame: truncate so future appends stay clean.
                    OpenOptions::new()
                        .write(true)
                        .open(&path)?
                        .set_len(good_off as u64)?;
                } else {
                    // Corruption inside a closed segment: later frames in this
                    // segment stay unreadable, but segments after it still
                    // replay. Surface it rather than dropping bytes silently.
                    tracing::warn!(
                        segment = %path.display(),
                        skipped_bytes = bytes.len() - good_off,
                        "WAL segment contains bytes after a corrupt frame"
                    );
                }
            }
            let mut max_ts: HashMap<String, u128> = HashMap::new();
            for (stream, _, record) in &seg_entries {
                let entry = max_ts.entry(stream.clone()).or_insert(0);
                *entry = (*entry).max(record.timestamp_ms);
            }
            if is_last {
                active_max_ts = max_ts;
            } else {
                closed.push(Segment { path, max_ts });
            }
            entries.append(&mut seg_entries);
        }

        let active_id = ids.last().copied().unwrap_or(1);
        let path = wal_dir.join(seg_name(active_id));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let active_bytes = file.metadata()?.len();

        Ok((
            Wal {
                dir: wal_dir,
                segment_max,
                closed,
                active_id,
                active: Some(BufWriter::new(file)),
                active_bytes,
                active_max_ts,
                poisoned: false,
            },
            entries,
        ))
    }

    /// Append one record frame; roll to a new segment first if the active one is
    /// full or was poisoned by a prior failed append.
    pub fn append(&mut self, stream: &str, shard_id: &str, record: &Record) -> io::Result<()> {
        if self.poisoned || self.active_bytes >= self.segment_max {
            self.roll()?;
        }
        let frame = encode_frame(stream, shard_id, record);
        if let Err(err) = self.write_frame(&frame) {
            // A partial write may have left a torn frame at the tail. Poison the
            // segment so the next append rolls to a fresh one: valid frames can
            // then never sit behind this garbage (decode stops at the first bad
            // frame, and last-segment truncate-repair would drop everything
            // after it). The failed record itself is reported to the caller.
            self.poisoned = true;
            return Err(err);
        }
        self.active_bytes += frame.len() as u64;
        let entry = self.active_max_ts.entry(stream.to_string()).or_insert(0);
        *entry = (*entry).max(record.timestamp_ms);
        Ok(())
    }

    fn active_writer(&mut self) -> io::Result<&mut BufWriter<File>> {
        self.active
            .as_mut()
            .ok_or_else(|| io::Error::other("WAL active writer is unavailable"))
    }

    fn write_frame(&mut self, frame: &[u8]) -> io::Result<()> {
        let active = self.active_writer()?;
        active.write_all(frame)?;
        active.flush() // to OS buffer; survives process crash (no fsync by design)
    }

    /// Delete closed segments whose records are all past their per-stream
    /// retention. The active segment is never dropped. `retentions` maps each
    /// current stream to its retention (seconds); a stream with retention 0, or
    /// one still present but never listed, keeps its segments forever. A stream
    /// absent from the map no longer exists, so its records won't replay and its
    /// segments are free to drop. Returns how many segments were deleted.
    pub fn drop_expired(
        &mut self,
        now_ms: u128,
        retentions: &HashMap<String, u64>,
    ) -> io::Result<usize> {
        let mut dropped = 0;
        let mut keep = Vec::with_capacity(self.closed.len());
        let mut first_err: Option<io::Error> = None;
        for seg in std::mem::take(&mut self.closed) {
            if !segment_expired(&seg, now_ms, retentions) {
                keep.push(seg);
                continue;
            }
            match fs::remove_file(&seg.path) {
                Ok(()) => dropped += 1,
                // The file is already gone: the entry is stale, so let it go
                // instead of retrying the removal every maintenance cycle.
                Err(err) if err.kind() == io::ErrorKind::NotFound => dropped += 1,
                // A real removal failure: keep the segment tracked so it stays a
                // drop candidate next cycle rather than leaking on disk. Record
                // the first error but keep processing the remaining segments.
                Err(err) => {
                    first_err.get_or_insert(err);
                    keep.push(seg);
                }
            }
        }
        self.closed = keep;
        match first_err {
            Some(err) => Err(err),
            None => Ok(dropped),
        }
    }

    fn roll(&mut self) -> io::Result<()> {
        let next_id = self.active_id + 1;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(seg_name(next_id)))?;
        let old = self
            .active
            .replace(BufWriter::new(file))
            .ok_or_else(|| io::Error::other("WAL active writer is unavailable"))?;
        if self.poisoned {
            // A failed append can leave the NACKed frame buffered (write_all
            // buffered it, flush failed). into_parts hands back the file without
            // flushing, so those bytes are dropped instead of landing at the
            // closing segment's tail on this roll (or on the writer's Drop),
            // where a transient error clearing would resurrect a record the
            // client was told had failed. Any prefix already flushed to the OS
            // becomes tail garbage that closed-segment replay tolerates.
            drop(old.into_parts());
        } else {
            old.into_inner().map_err(io::IntoInnerError::into_error)?;
        }
        self.closed.push(Segment {
            path: self.dir.join(seg_name(self.active_id)),
            max_ts: std::mem::take(&mut self.active_max_ts),
        });
        self.active_id = next_id;
        self.active_bytes = 0;
        self.poisoned = false;
        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        if !self.poisoned {
            return;
        }
        if let Some(active) = self.active.take() {
            drop(active.into_parts());
        }
    }
}

/// A closed segment is droppable once every stream it holds records for is safe
/// to drop: the stream is gone (its records won't replay), or has a finite
/// retention its newest record here has outlived. Retention 0 (keep forever)
/// pins the segment.
fn segment_expired(seg: &Segment, now_ms: u128, retentions: &HashMap<String, u64>) -> bool {
    seg.max_ts
        .iter()
        .all(|(stream, &max_ts)| match retentions.get(stream).copied() {
            None => true,
            Some(0) => false,
            Some(retention) => now_ms.saturating_sub(max_ts) > u128::from(retention) * 1000,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(seq: u64, data: Vec<u8>) -> Record {
        Record {
            seq,
            partition_key: "pk".into(),
            data,
            timestamp_ms: 7,
        }
    }

    fn tmp_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("fs-wal-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn frame_round_trips() {
        let mut buf = encode_frame("S", "shardId-000000000000", &rec(1, vec![9, 9, 9]));
        buf.extend(encode_frame("S", "shardId-000000000000", &rec(2, vec![8])));
        let (entries, off) = decode_segment(&buf);
        assert_eq!(off, buf.len());
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].2.seq, 1);
        assert_eq!(entries[1].2.data, vec![8]);
    }

    #[test]
    fn torn_trailing_frame_is_dropped_at_safe_offset() {
        let mut buf = encode_frame("S", "shardId-000000000000", &rec(1, vec![1, 2, 3]));
        let good_len = buf.len();
        let torn = encode_frame("S", "shardId-000000000000", &rec(2, vec![0u8; 50]));
        buf.extend_from_slice(&torn[..torn.len() - 10]); // chop the tail
        let (entries, off) = decode_segment(&buf);
        assert_eq!(entries.len(), 1);
        assert_eq!(off, good_len); // truncate here to keep appends clean
    }

    #[test]
    fn append_then_reload_returns_entries() {
        let dir = tmp_dir("reload");
        {
            let (mut wal, entries) = Wal::load(&dir, 1 << 20).unwrap();
            assert!(entries.is_empty());
            wal.append("S", "shardId-000000000000", &rec(1, vec![1]))
                .unwrap();
            wal.append("S", "shardId-000000000000", &rec(2, vec![2]))
                .unwrap();
        }
        let (_wal, entries) = Wal::load(&dir, 1 << 20).unwrap();
        assert_eq!(
            entries.iter().map(|(_, _, r)| r.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rolls_to_new_segment_when_full() {
        let dir = tmp_dir("roll");
        let (mut wal, _) = Wal::load(&dir, 32).unwrap(); // tiny cap forces a roll
        wal.append("S", "shardId-000000000000", &rec(1, vec![0u8; 40]))
            .unwrap();
        wal.append("S", "shardId-000000000000", &rec(2, vec![0u8; 40]))
            .unwrap();
        let segs = fs::read_dir(dir.join("wal")).unwrap().count();
        assert!(segs >= 2, "expected a roll, found {segs} segment(s)");
        let _ = fs::remove_dir_all(&dir);
    }

    use crate::store::Store;

    #[test]
    fn store_survives_reload_through_manifest_and_wal() {
        let dir = tmp_dir("e2e");
        // first "process": create stream, put records via the store + WAL
        {
            let mut store = Store::new(86_400);
            store.create_stream("S", 1, None);
            crate::manifest::save(&dir, &store).unwrap();
            let (mut wal, _) = Wal::load(&dir, 1 << 20).unwrap();
            for i in 0..50u64 {
                let (shard_id, _seq) = store
                    .put("S", format!("pk{i}"), vec![i as u8; 100], None)
                    .unwrap();
                let r = store.last_record("S", &shard_id).unwrap();
                wal.append("S", &shard_id, r).unwrap();
            }
            crate::manifest::save(&dir, &store).unwrap();
        }
        // second "process": reload
        let mut reloaded = crate::manifest::load(&dir).unwrap();
        let (_wal, entries) = Wal::load(&dir, 1 << 20).unwrap();
        let mut max_seq = 0;
        for (s, sh, r) in entries {
            max_seq = max_seq.max(r.seq);
            reloaded.restore_record(&s, &sh, r);
        }
        reloaded.bump_seq_to(max_seq);
        assert_eq!(
            reloaded
                .stream_sizes()
                .iter()
                .map(|(_, n, _)| n)
                .sum::<u64>(),
            50
        );
        // new puts continue above the replayed high-water
        let (_, seq) = reloaded.put("S", "pk".into(), vec![0], None).unwrap();
        assert_eq!(seq, 51);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn recreated_stream_drops_records_predating_creation() {
        let dir = tmp_dir("resurrect");
        let shard = "shardId-000000000000";
        // first "process": S existed, took records, was deleted, then recreated
        // under the same name (shard IDs are deterministic, so they collide).
        // The append-only WAL still holds the deleted incarnation's records.
        {
            let mut store = Store::new(86_400);
            store.create_stream("S", 1, None);
            // The first incarnation assigned seqs 1..=3; advance the global
            // counter the way real puts would.
            store.bump_seq_to(3);
            let (mut wal, _) = Wal::load(&dir, 1 << 20).unwrap();
            for seq in 1..=3u64 {
                let old = Record {
                    seq,
                    partition_key: "old".into(),
                    data: vec![seq as u8],
                    timestamp_ms: 1_000,
                };
                wal.append("S", shard, &old).unwrap();
            }
            store.streams.remove("S");
            // Recreate under the same name: the seq floor is now 3. Delete +
            // recreate landed in the same millisecond as the old incarnation's
            // records — created_ms equals their timestamps, so wall time cannot
            // separate the incarnations; only the seq floor (persisted in the
            // manifest) can.
            store.create_stream("S", 1, None);
            store.streams.get_mut("S").unwrap().created_ms = 1_000;
            // seq 4 lands in the same millisecond as creation and must be kept;
            // seq 5 clearly post-dates it.
            for (seq, ts) in [(4u64, 1_000u128), (5, 2_000)] {
                let new = Record {
                    seq,
                    partition_key: "new".into(),
                    data: vec![seq as u8],
                    timestamp_ms: ts,
                };
                wal.append("S", shard, &new).unwrap();
            }
            crate::manifest::save(&dir, &store).unwrap();
        }
        // second "process": reload via manifest + WAL replay.
        let mut reloaded = crate::manifest::load(&dir).unwrap();
        let (_wal, entries) = Wal::load(&dir, 1 << 20).unwrap();
        for (s, sh, r) in entries {
            reloaded.restore_record(&s, &sh, r);
        }
        assert_eq!(
            reloaded
                .stream_sizes()
                .iter()
                .map(|(_, n, _)| n)
                .sum::<u64>(),
            2,
            "old records must not resurrect; boundary + newer records survive"
        );
        let last = reloaded.last_record("S", shard).unwrap();
        assert_eq!(last.seq, 5);
        assert_eq!(last.partition_key, "new");
        let _ = fs::remove_dir_all(&dir);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn replay_reconstructs_records(
            payloads in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 0..64),
                0..40,
            )
        ) {
            let dir = tmp_dir(&format!("prop-{}", payloads.len()));
            let mut source = Store::new(86_400);
            source.create_stream("S", 1, None);
            crate::manifest::save(&dir, &source).unwrap();
            let (mut wal, _) = Wal::load(&dir, 256).unwrap(); // small cap -> exercises rolls
            for p in &payloads {
                let (shard_id, _) =
                    source.put("S", "pk".into(), p.clone(), None).unwrap();
                wal.append("S", &shard_id, source.last_record("S", &shard_id).unwrap())
                    .unwrap();
            }
            drop(wal);
            let (_wal, entries) = Wal::load(&dir, 256).unwrap();
            let replayed: Vec<Vec<u8>> = entries.iter().map(|(_, _, r)| r.data.clone()).collect();
            prop_assert_eq!(replayed, payloads);
            let _ = fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn drops_only_fully_expired_closed_segments() {
        let dir = tmp_dir("drop");
        let (mut wal, _) = Wal::load(&dir, 16).unwrap(); // tiny -> each append rolls
                                                         // segment 1: old record (ts=1000), then a roll
        wal.append(
            "S",
            "shardId-000000000000",
            &Record {
                seq: 1,
                partition_key: "p".into(),
                data: vec![0u8; 20],
                timestamp_ms: 1_000,
            },
        )
        .unwrap();
        // segment 2: fresh record (ts=now)
        wal.append(
            "S",
            "shardId-000000000000",
            &Record {
                seq: 2,
                partition_key: "p".into(),
                data: vec![0u8; 20],
                timestamp_ms: 10_000_000,
            },
        )
        .unwrap();
        // now = 10_000_000 ms, retention 1s (1000 ms): segment 1 (max_ts 1000) is expired, segment 2 is not.
        let retentions = HashMap::from([("S".to_string(), 1u64)]);
        let dropped = wal.drop_expired(10_000_000, &retentions).unwrap();
        assert_eq!(dropped, 1);
        // active segment is never dropped even if it looks old
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn drop_expired_keeps_bookkeeping_when_removal_fails() {
        let dir = tmp_dir("drop-err");
        let (mut wal, _) = Wal::load(&dir, 1 << 20).unwrap();
        let wal_dir = dir.join(SUBDIR);
        let ok1 = wal_dir.join("seg-ok1.log");
        let bad = wal_dir.join("seg-bad.log");
        let ok2 = wal_dir.join("seg-ok2.log");
        let gone = wal_dir.join("seg-gone.log");
        fs::write(&ok1, b"x").unwrap();
        fs::create_dir(&bad).unwrap(); // remove_file on a dir errors (not NotFound)
        fs::write(&ok2, b"x").unwrap();
        // `gone` is never created, so its removal hits NotFound.
        let ts = || HashMap::from([("S".to_string(), 1u128)]);
        wal.closed = vec![
            Segment {
                path: ok1.clone(),
                max_ts: ts(),
            },
            Segment {
                path: bad.clone(),
                max_ts: ts(),
            },
            Segment {
                path: ok2.clone(),
                max_ts: ts(),
            },
            Segment {
                path: gone.clone(),
                max_ts: ts(),
            },
        ];

        // now far in the future, retention 1s: every segment is expired.
        let retentions = HashMap::from([("S".to_string(), 1u64)]);
        let err = wal.drop_expired(1_000_000_000, &retentions).unwrap_err();
        assert_ne!(err.kind(), io::ErrorKind::NotFound);
        // Removable files are gone; the failed segment stays tracked; the
        // already-missing segment is dropped rather than wedging the list.
        assert!(!ok1.exists());
        assert!(!ok2.exists());
        assert_eq!(wal.closed.len(), 1);
        assert_eq!(wal.closed[0].path, bad);

        // Clear the failure, then the survivor drops cleanly on the next call.
        fs::remove_dir(&bad).unwrap();
        assert_eq!(wal.drop_expired(1_000_000_000, &retentions).unwrap(), 1);
        assert!(wal.closed.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn drops_segments_per_stream_not_by_global_retention() {
        let dir = tmp_dir("per-stream");
        let (mut wal, _) = Wal::load(&dir, 16).unwrap(); // tiny cap: each record its own segment
        wal_append(&mut wal, "EXP", 1, 1_000); // segment 1: only the 1-second stream's old record
        wal_append(&mut wal, "KEEP", 2, 1_000); // segment 2: only the keep-forever stream's old record
        wal_append(&mut wal, "EXP", 3, 10_000_000); // active segment keeps 1 & 2 closed

        // EXP retains 1s, KEEP retains forever (0). At now=10_000_000 the EXP
        // segment is long expired while the KEEP segment must survive.
        let retentions = HashMap::from([("EXP".to_string(), 1u64), ("KEEP".to_string(), 0u64)]);
        let dropped = wal.drop_expired(10_000_000, &retentions).unwrap();
        assert_eq!(dropped, 1, "only the expired stream's segment drops");
        assert_eq!(
            wal.closed.len(),
            1,
            "the keep-forever segment stays tracked"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    fn wal_append(wal: &mut Wal, stream: &str, seq: u64, timestamp_ms: u128) {
        wal.append(
            stream,
            "shardId-000000000000",
            &Record {
                seq,
                partition_key: "p".into(),
                data: vec![0u8; 20],
                timestamp_ms,
            },
        )
        .unwrap();
    }

    #[test]
    fn poisoned_segment_rolls_before_next_append() {
        let dir = tmp_dir("poison");
        let (mut wal, _) = Wal::load(&dir, 1 << 20).unwrap(); // large cap: no natural roll
        wal.append("S", "shardId-000000000000", &rec(1, vec![1]))
            .unwrap();
        let before = fs::read_dir(dir.join(SUBDIR)).unwrap().count();
        // Stand in for a failed append having torn the active segment.
        wal.poisoned = true;
        wal.append("S", "shardId-000000000000", &rec(2, vec![2]))
            .unwrap();
        let after = fs::read_dir(dir.join(SUBDIR)).unwrap().count();
        assert_eq!(
            after,
            before + 1,
            "poisoned segment must roll to a fresh one"
        );
        drop(wal);
        let (_w, entries) = Wal::load(&dir, 1 << 20).unwrap();
        assert_eq!(
            entries.iter().map(|(_, _, r)| r.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn poisoned_roll_discards_buffered_nacked_frame() {
        let dir = tmp_dir("poison-discard");
        let (mut wal, _) = Wal::load(&dir, 1 << 20).unwrap(); // large cap: no natural roll
        wal.append("S", "shardId-000000000000", &rec(1, vec![1]))
            .unwrap();
        // Stand in for a failed append: write_all buffered the frame, flush then
        // failed, so the bytes sit unflushed in the writer and the segment is
        // poisoned. Writing without flushing leaves the frame in the BufWriter.
        let nacked = encode_frame("S", "shardId-000000000000", &rec(2, vec![0xEE; 32]));
        wal.active_writer().unwrap().write_all(&nacked).unwrap();
        wal.poisoned = true;
        // The next append rolls; the buffered NACKed frame must be discarded, not
        // flushed onto the closing segment's tail.
        wal.append("S", "shardId-000000000000", &rec(3, vec![3]))
            .unwrap();
        drop(wal);
        let (_w, entries) = Wal::load(&dir, 1 << 20).unwrap();
        assert_eq!(
            entries.iter().map(|(_, _, r)| r.seq).collect::<Vec<_>>(),
            vec![1, 3],
            "the NACKed record must not resurrect on replay"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn poisoned_drop_discards_buffered_nacked_frame() {
        let dir = tmp_dir("poison-drop");
        let (mut wal, _) = Wal::load(&dir, 1 << 20).unwrap();
        wal.append("S", "shardId-000000000000", &rec(1, vec![1]))
            .unwrap();

        let nacked = encode_frame("S", "shardId-000000000000", &rec(2, vec![0xEE; 32]));
        wal.active_writer().unwrap().write_all(&nacked).unwrap();
        wal.poisoned = true;
        drop(wal);

        let (_wal, entries) = Wal::load(&dir, 1 << 20).unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|(_, _, record)| record.seq)
                .collect::<Vec<_>>(),
            vec![1],
            "dropping a poisoned WAL must not flush the NACKed record"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupted_closed_segment_does_not_hide_later_segments() {
        let dir = tmp_dir("corrupt-mid");
        let (mut wal, _) = Wal::load(&dir, 16).unwrap(); // tiny cap: each record its own segment
        for seq in 1..=3u64 {
            wal.append("S", "shardId-000000000000", &rec(seq, vec![seq as u8; 20]))
                .unwrap();
        }
        drop(wal);
        let mut segs: Vec<PathBuf> = fs::read_dir(dir.join(SUBDIR))
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "log"))
            .collect();
        segs.sort();
        // Corrupt a closed middle segment; the active (last) segment is segs[2].
        fs::write(&segs[1], b"garbage-not-a-valid-frame").unwrap();
        let (_w, entries) = Wal::load(&dir, 16).unwrap();
        let seqs: Vec<u64> = entries.iter().map(|(_, _, r)| r.seq).collect();
        assert!(seqs.contains(&1));
        assert!(
            seqs.contains(&3),
            "a later segment must still replay past a corrupt one"
        );
        assert!(
            !seqs.contains(&2),
            "the corrupt segment's record is dropped"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}

//! The stream-definition manifest: stream metadata + the seq high-water mark,
//! written atomically and fsynced for crash durability. Records live in the WAL,
//! not here (`Shard.records` is `#[serde(skip)]`), so the manifest stays tiny and
//! is safe to rewrite often.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use crate::store::Store;

const FILE: &str = "manifest.json";

/// Atomically write and fsync the stream definitions + seq counter to
/// `<dir>/manifest.json`. The temp file's data is flushed to disk before the
/// rename, and (on unix) the directory is fsynced after so the rename itself is
/// durable — without this a crash could journal the rename ahead of the data and
/// leave a zero-length manifest, losing every stream definition.
pub fn save(dir: &Path, store: &Store) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = dir.join(FILE);
    let tmp = dir.join(format!("{FILE}.tmp"));
    let bytes = serde_json::to_vec(store).map_err(io::Error::other)?;
    let mut file = File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, &path)?;
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Load stream definitions (records empty — they come from the WAL). Returns
/// `None` if the manifest is missing or unparseable (degrade to empty store).
pub fn load(dir: &Path) -> Option<Store> {
    let bytes = fs::read(dir.join(FILE)).ok()?;
    match serde_json::from_slice(&bytes) {
        Ok(store) => Some(store),
        Err(err) => {
            tracing::warn!(error = %err, "ignoring unreadable manifest");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[test]
    fn save_then_load_round_trips_streams_and_seq() {
        let dir = std::env::temp_dir().join(format!("fs-manifest-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut store = Store::new(86_400);
        store.create_stream("API-TRANSACTIONS", 1, None);
        store.put("API-TRANSACTIONS", "p".into(), vec![0u8; 10], None);
        save(&dir, &store).unwrap();

        let loaded = load(&dir).unwrap();
        assert!(loaded.streams.contains_key("API-TRANSACTIONS"));
        // records are NOT in the manifest
        assert_eq!(
            loaded.stream_sizes().iter().map(|(_, n, _)| n).sum::<u64>(),
            0
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_missing_dir_is_none() {
        let dir = std::env::temp_dir().join("fs-manifest-does-not-exist-xyz");
        assert!(load(&dir).is_none());
    }
}

# PR #5 Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the residual WAL, stream-identifier, Java documentation, and lint-policy findings from the read-only review of PR #5.

**Architecture:** Keep the current concrete WAL and operation-handler architecture. Make the active WAL writer ownable during roll and destruction, validate stream identifiers at the JSON/API boundary without a regex dependency, and adopt one process-wide tracing subscriber while converting existing panic/logging sites to policy-compliant handling.

**Tech Stack:** Rust 1.90, `std`, `serde_json`, `postcard`, `tracing == 0.1.44`, `tracing-subscriber == 0.3.23`, Cargo/Clippy, GitHub Actions.

## Global Constraints

- Work on the current `audit-fixes` branch; do not create or switch branches.
- Preserve the untracked `benchmark/` directory and never stage it.
- Do not push; commits remain local until the user approves publication.
- Keep functions at or below 100 lines, cyclomatic complexity at or below 8, and lines at or below 100 characters.
- Do not add a regex dependency or genericize `Wal` over writer types.
- Pin new dependency versions exactly.
- Keep the lock order store then WAL.
- Use TDD for every behavior change: demonstrate the new test failing before implementation.
- Every commit must be signed when available and end with `Co-Authored-By: Claude <noreply@anthropic.com>`.

---

## File Map

- `src/wal.rs`: own/discard the active writer safely during roll and `Drop`; add poisoned-drop regression coverage; correct `drop_expired` contract text.
- `src/ops.rs`: validate identifier JSON types and complete Kinesis ARN structure; add behavior tests.
- `src/main.rs`: initialize tracing, replace startup/error prints, and centralize poisoned lock recovery.
- `src/manifest.rs`: replace manifest warning output with tracing.
- `src/protocol.rs`: narrowly justify the structurally infallible base64 `expect()`.
- `Cargo.toml`: add pinned tracing dependencies and enforce expect/stdout/stderr lint levels.
- `Cargo.lock`: record exact dependency resolution.
- `README.md`: split Java SDK v1/v2 CBOR instructions.
- `docs/DESIGN.md`: describe per-stream mixed-segment WAL collection accurately.

---

### Task 1: Prevent poisoned WAL destruction from flushing NACKed frames

**Files:**
- Modify: `src/wal.rs:74-260`
- Test: `src/wal.rs:612-675`

**Interfaces:**
- Consumes: existing `Wal::append`, `Wal::roll`, `encode_frame`, and `Wal::load`.
- Produces: `Wal::active_writer(&mut self) -> io::Result<&mut BufWriter<File>>`; `Wal` owns `active: Option<BufWriter<File>>`; `Drop for Wal` discards a poisoned buffer.

- [ ] **Step 1: Add the failing poisoned-drop regression test**

Add this test next to `poisoned_roll_discards_buffered_nacked_frame`:

```rust
#[test]
fn poisoned_drop_discards_buffered_nacked_frame() {
    let dir = tmp_dir("poison-drop");
    let (mut wal, _) = Wal::load(&dir, 1 << 20).unwrap();
    wal.append("S", "shardId-000000000000", &rec(1, vec![1]))
        .unwrap();

    let nacked = encode_frame("S", "shardId-000000000000", &rec(2, vec![0xEE; 32]));
    wal.active.write_all(&nacked).unwrap();
    wal.poisoned = true;
    drop(wal);

    let (_wal, entries) = Wal::load(&dir, 1 << 20).unwrap();
    assert_eq!(
        entries.iter().map(|(_, _, record)| record.seq).collect::<Vec<_>>(),
        vec![1],
        "dropping a poisoned WAL must not flush the NACKed record"
    );
    let _ = fs::remove_dir_all(&dir);
}
```

This test intentionally uses the current concrete private writer, matching the existing poisoned-roll test rather than introducing writer generics.

- [ ] **Step 2: Run the new test and verify it fails**

Run:

```bash
cargo test wal::tests::poisoned_drop_discards_buffered_nacked_frame -- --exact
```

Expected: FAIL because sequence `2` is flushed by `BufWriter::drop` and replayed.

- [ ] **Step 3: Make the active writer ownable**

Change the field and constructor:

```rust
pub struct Wal {
    dir: PathBuf,
    segment_max: u64,
    closed: Vec<Segment>,
    active_id: u64,
    active: Option<BufWriter<File>>,
    active_bytes: u64,
    active_max_ts: HashMap<String, u128>,
    poisoned: bool,
}
```

In `Wal::load` initialize it with:

```rust
active: Some(BufWriter::new(file)),
```

Add this helper inside `impl Wal`:

```rust
fn active_writer(&mut self) -> io::Result<&mut BufWriter<File>> {
    self.active
        .as_mut()
        .ok_or_else(|| io::Error::other("WAL active writer is unavailable"))
}
```

Update `write_frame`:

```rust
fn write_frame(&mut self, frame: &[u8]) -> io::Result<()> {
    let active = self.active_writer()?;
    active.write_all(frame)?;
    active.flush()
}
```

- [ ] **Step 4: Update roll and destruction semantics**

Replace the active-writer portion of `roll()` with:

```rust
let old = self
    .active
    .replace(BufWriter::new(file))
    .ok_or_else(|| io::Error::other("WAL active writer is unavailable"))?;
if self.poisoned {
    drop(old.into_parts());
} else {
    old.into_inner().map_err(io::IntoInnerError::into_error)?;
}
```

Add after the `impl Wal` block:

```rust
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
```

Update the two in-module tests that access `wal.active` directly:

```rust
wal.active_writer().unwrap().write_all(&nacked).unwrap();
```

- [ ] **Step 5: Run the focused WAL tests**

Run:

```bash
cargo test poisoned_ -- --nocapture
```

Expected: all poisoned-segment tests PASS, including both later-roll and immediate-drop paths.

- [ ] **Step 6: Mutation-check the regression test**

Temporarily remove or neutralize the `Drop for Wal` body, run:

```bash
cargo test wal::tests::poisoned_drop_discards_buffered_nacked_frame -- --exact
```

Expected: FAIL with sequence `2` present. Restore the implementation immediately and rerun the test to PASS. Use `Edit` for the temporary mutation; do not commit the broken state.

- [ ] **Step 7: Commit the WAL fix**

```bash
git add src/wal.rs
git commit -m "Discard buffered NACKed frame when dropping poisoned WAL" \
  -m "Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: Validate complete, correctly typed stream identifiers

**Files:**
- Modify: `src/ops.rs:22-63`
- Test: `src/ops.rs:670-715`

**Interfaces:**
- Consumes: `ApiError::validation`, `validate_stream_name`, and all handlers that call `resolve_stream_name`.
- Produces: `arn_stream_name(arn: &str) -> Option<&str>` validating the complete ARN; `resolve_stream_name(req: &Value) -> Result<&str, ApiError>` rejecting wrong JSON types.

- [ ] **Step 1: Add failing tests for malformed ARN structure**

Add:

```rust
#[test]
fn describe_stream_rejects_substring_only_arn() {
    let store = store_with_stream();
    let error = describe_stream(&store, &json!({ "StreamARN": "garbage:stream/S" }))
        .unwrap_err();
    assert_eq!(error.kind, "ValidationException");
}

#[test]
fn describe_stream_rejects_extra_arn_resource_components() {
    let store = store_with_stream();
    let arn = "arn:aws:kinesis:us-east-1:000000000000:stream/other/S";
    let error = describe_stream(&store, &json!({ "StreamARN": arn })).unwrap_err();
    assert_eq!(error.kind, "ValidationException");
}
```

- [ ] **Step 2: Add failing tests for wrong JSON types**

Add:

```rust
#[test]
fn describe_stream_rejects_non_string_stream_name() {
    let store = store_with_stream();
    let arn = store.streams["S"].arn.clone();
    let error = describe_stream(
        &store,
        &json!({ "StreamName": 123, "StreamARN": arn }),
    )
    .unwrap_err();
    assert_eq!(error.kind, "ValidationException");
}

#[test]
fn describe_stream_rejects_non_string_stream_arn() {
    let store = store_with_stream();
    let error = describe_stream(
        &store,
        &json!({ "StreamName": "S", "StreamARN": 123 }),
    )
    .unwrap_err();
    assert_eq!(error.kind, "ValidationException");
}
```

- [ ] **Step 3: Run the new identifier tests and verify they fail**

Run:

```bash
cargo test describe_stream_rejects_ -- --nocapture
```

Expected: the existing malformed/mismatch test passes; the four new tests FAIL because malformed/wrong-typed identifiers currently resolve successfully.

- [ ] **Step 4: Validate the complete ARN shape without a dependency**

Replace `arn_stream_name` with:

```rust
fn arn_stream_name(arn: &str) -> Option<&str> {
    let mut fields = arn.splitn(6, ':');
    let prefix = fields.next()?;
    let partition = fields.next()?;
    let service = fields.next()?;
    let region = fields.next()?;
    let account = fields.next()?;
    let resource = fields.next()?;

    if prefix != "arn"
        || !(partition == "aws" || partition.starts_with("aws-"))
        || service != "kinesis"
        || region.is_empty()
        || account.len() != 12
        || !account.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let name = resource.strip_prefix("stream/")?;
    if name.is_empty() || name.contains('/') || validate_stream_name(name).is_err() {
        return None;
    }
    Some(name)
}
```

This accepts standard, GovCloud, and China AWS partitions while rejecting arbitrary prefixes, wrong services, empty regions, non-account IDs, and extra resource components.

- [ ] **Step 5: Distinguish absent identifiers from wrong types**

Replace the first two lines of `resolve_stream_name` with explicit matching:

```rust
let name = match req.get("StreamName") {
    None => None,
    Some(Value::String(name)) => Some(name.as_str()),
    Some(_) => {
        return Err(ApiError::validation(
            "StreamName must be a string when provided",
        ));
    }
};
let arn = match req.get("StreamARN") {
    None => None,
    Some(Value::String(arn)) => Some(arn.as_str()),
    Some(_) => {
        return Err(ApiError::validation(
            "StreamARN must be a string when provided",
        ));
    }
};
```

Keep the existing four-way match and mismatch behavior unchanged.

- [ ] **Step 6: Run identifier tests**

Run:

```bash
cargo test describe_stream_ -- --nocapture
```

Expected: all ARN-only, matching pair, mismatch, malformed ARN, extra component, and wrong-type tests PASS.

- [ ] **Step 7: Run all operation tests**

```bash
cargo test ops::tests -- --nocapture
```

Expected: PASS with no changed behavior for existing data-plane handlers.

- [ ] **Step 8: Commit identifier validation**

```bash
git add src/ops.rs
git commit -m "Validate stream identifier types and ARN structure" \
  -m "Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: Adopt tracing and enforce stdout/stderr lints

**Files:**
- Modify: `Cargo.toml:14-22,35-50`
- Modify: `Cargo.lock`
- Modify: `src/main.rs:1-220,387-440`
- Modify: `src/manifest.rs:32-45`
- Modify: `src/ops.rs:205-290`
- Modify: `src/wal.rs:112-140`

**Interfaces:**
- Produces: one process-wide `tracing-subscriber` formatter; `read`, `write`, and `lock_wal` helpers log and abort on poisoned locks.
- Dependency versions: `tracing == 0.1.44`; `tracing-subscriber == 0.3.23`.

- [ ] **Step 1: Add exact tracing dependencies**

Add under `[dependencies]`:

```toml
tracing = "=0.1.44"
tracing-subscriber = "=0.3.23"
```

Run:

```bash
cargo check
```

Expected: PASS and `Cargo.lock` updated with tracing dependencies.

- [ ] **Step 2: Initialize tracing and make startup failure explicit**

Change `main` to return a result and initialize the formatter before configuration loading:

```rust
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .without_time()
        .init();

    let config = Config::load();
    let addr = format!("0.0.0.0:{}", config.port);
    let server = Arc::new(Server::http(&addr)?);
```

Replace startup output with:

```rust
tracing::info!(address = %addr, "fakestream listening (Kinesis, AWS JSON 1.1)");
tracing::info!(retention_secs = config.default_retention_secs, "default retention");
match &persist_dir {
    Some(dir) => tracing::info!(path = %dir, "persistence enabled"),
    None => tracing::info!("persistence disabled"),
}
```

End `main` with:

```rust
Ok(())
```

- [ ] **Step 3: Replace production stderr/stdout calls**

Use the following severity mapping:

```rust
tracing::warn!(error = %err, "WAL open failed; starting without persistence");
tracing::info!(removed, "replayed WAL and trimmed expired records");
tracing::error!(error = %err, "manifest save failed");
tracing::error!(error = %err, "WAL segment drop failed");
tracing::error!(error = %err, "WAL append failed");
tracing::warn!(
    segment = %path.display(),
    skipped_bytes = bytes.len() - good_off,
    "WAL segment contains bytes after a corrupt frame"
);
tracing::warn!(error = %err, "ignoring unreadable manifest");
```

Apply these replacements in `src/main.rs`, `src/manifest.rs`, `src/ops.rs`, and `src/wal.rs`. Do not add logging flags or environment-filter behavior.

- [ ] **Step 4: Centralize fail-fast poisoned lock handling**

Import guard types in `src/main.rs`:

```rust
use std::sync::{
    Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
```

Replace the existing `read` and `write` helpers and add `lock_wal`:

```rust
fn write(store: &RwLock<Store>) -> RwLockWriteGuard<'_, Store> {
    store.write().unwrap_or_else(|_| {
        tracing::error!("store write lock poisoned; aborting");
        std::process::abort();
    })
}

fn read(store: &RwLock<Store>) -> RwLockReadGuard<'_, Store> {
    store.read().unwrap_or_else(|_| {
        tracing::error!("store read lock poisoned; aborting");
        std::process::abort();
    })
}

fn lock_wal(wal: &Mutex<Wal>) -> MutexGuard<'_, Wal> {
    wal.lock().unwrap_or_else(|_| {
        tracing::error!("WAL lock poisoned; aborting");
        std::process::abort();
    })
}
```

This preserves the existing stop-on-poison behavior without continuing with potentially inconsistent state or using a panic macro. Use `write(&store)` in maintenance, `read(store)` in `persist_and_gc`, `lock_wal(wal)` in `persist_and_gc`, and `lock_wal(w)` in `with_wal`. Preserve store-then-WAL acquisition order.

- [ ] **Step 5: Enforce stdout/stderr policy**

Change `Cargo.toml`:

```toml
print_stdout = "deny"
print_stderr = "deny"
```

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: no print lint diagnostics. Other `expect_used` diagnostics remain for Task 4.

- [ ] **Step 6: Run tests after logging migration**

```bash
cargo test --all-features
```

Expected: all tests PASS. Output format is allowed to change from plain startup lines to tracing events; API behavior remains unchanged.

- [ ] **Step 7: Commit tracing migration**

```bash
git add Cargo.toml Cargo.lock src/main.rs src/manifest.rs src/ops.rs src/wal.rs
git commit -m "Use tracing for application logging" \
  -m "Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: Enforce the expect policy with narrow justified exceptions

**Files:**
- Modify: `Cargo.toml:35-50`
- Modify: `src/main.rs:225-440`
- Modify: `src/ops.rs:515-540`
- Modify: `src/protocol.rs:68-78`
- Modify: `src/wal.rs:20-55`

**Interfaces:**
- Produces: `expect_used = "warn"` with no unqualified production `expect()` calls under strict CI.

- [ ] **Step 1: Enable the expect lint and observe failures**

Change:

```toml
expect_used = "warn"
```

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: FAIL at the remaining production `expect()` sites.

- [ ] **Step 2: Remove the slice conversion expect**

In `decode_segment`, replace:

```rust
let len = u64::from_le_bytes(bytes[off..off + 8].try_into().expect("8 bytes")) as usize;
```

with:

```rust
let mut length_bytes = [0u8; 8];
length_bytes.copy_from_slice(&bytes[off..off + 8]);
let len = u64::from_le_bytes(length_bytes) as usize;
```

- [ ] **Step 3: Mark structurally infallible boundaries explicitly**

Add a narrow `#[expect]` directly above `encode_frame`:

```rust
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
```

Add narrow attributes directly above the existing `write_record` and
`write_json_string` bodies:

```rust
#[expect(
    clippy::expect_used,
    reason = "serde_json writes to Vec<u8>, whose Write implementation is infallible"
)]
fn write_record(out: &mut Vec<u8>, record: &Record) {
    out.extend_from_slice(b"{\"SequenceNumber\":\"");
    out.extend_from_slice(record.seq.to_string().as_bytes());
    out.extend_from_slice(b"\",\"ApproximateArrivalTimestamp\":");
    let timestamp = (record.timestamp_ms / 1000) as f64;
    serde_json::to_writer(&mut *out, &timestamp).expect("f64 serialization is infallible");
    out.extend_from_slice(b",\"PartitionKey\":");
    write_json_string(out, &record.partition_key);
    out.extend_from_slice(b",\"Data\":\"");
    encode_data_into(&record.data, out);
    out.extend_from_slice(b"\"}");
}

#[expect(
    clippy::expect_used,
    reason = "serde_json writes to Vec<u8>, whose Write implementation is infallible"
)]
fn write_json_string(out: &mut Vec<u8>, value: &str) {
    serde_json::to_writer(&mut *out, value).expect("string serialization is infallible");
}
```

Add a narrow attribute above `encode_data_into`:

```rust
#[expect(
    clippy::expect_used,
    reason = "the output slice is sized to the exact padded base64 length"
)]
pub fn encode_data_into(bytes: &[u8], out: &mut Vec<u8>) {
    let need = bytes.len().div_ceil(3) * 4;
    let start = out.len();
    out.resize(start + need, 0);
    let written = B64
        .encode_slice(bytes, &mut out[start..])
        .expect("output buffer is exactly pre-sized for base64");
    debug_assert_eq!(written, need);
}
```

Extract the metrics header from `handle` and isolate the static header expects:

```rust
#[expect(
    clippy::expect_used,
    reason = "fixed Content-Type bytes are a valid HTTP header"
)]
fn metrics_header() -> Header {
    Header::from_bytes(&b"Content-Type"[..], &b"text/plain; version=0.0.4"[..])
        .expect("static header is valid")
}

#[expect(
    clippy::expect_used,
    reason = "fixed Content-Type bytes are a valid HTTP header"
)]
fn json_header() -> Header {
    Header::from_bytes(&b"Content-Type"[..], &b"application/x-amz-json-1.1"[..])
        .expect("static header is valid")
}

#[expect(
    clippy::expect_used,
    reason = "ApiError kinds are fixed AWS exception names valid in an HTTP header"
)]
fn respond_error(request: Request, err: &ApiError) {
    let error_type = Header::from_bytes(&b"x-amzn-errortype"[..], err.kind.as_bytes())
        .expect("error kind is valid header value");
    let response = Response::from_string(err.body())
        .with_status_code(err.status)
        .with_header(json_header())
        .with_header(error_type);
    let _ = request.respond(response);
}
```

Change the metrics response in `handle` to `with_header(metrics_header())`.
Do not use `#[allow]`; `allow_attributes = "deny"` remains enforced.

- [ ] **Step 4: Confirm no production expect remains unaccounted for**

Run:

```bash
cymbal search '.expect(' --text
```

Expected: only the structurally infallible sites documented by adjacent `#[expect]` attributes. Lock and server startup expects must be gone.

- [ ] **Step 5: Run strict Clippy and tests**

```bash
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

Expected: both PASS with no warnings.

- [ ] **Step 6: Commit lint enforcement**

```bash
git add Cargo.toml src/main.rs src/ops.rs src/protocol.rs src/wal.rs
git commit -m "Enforce panic and output lint policy" \
  -m "Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: Correct Java and WAL documentation

**Files:**
- Modify: `README.md:145-156`
- Modify: `docs/DESIGN.md:76-82`
- Modify: `src/wal.rs:197-202`

**Interfaces:**
- Documents the behavior implemented by Tasks 1-4; no runtime interface changes.

- [ ] **Step 1: Split Java SDK CBOR instructions**

Replace the combined Java bullet with this lead-in and two generation-specific bullets:

```markdown
Java SDK clients default to CBOR (`application/x-amz-cbor-1.1`), which fakestream does not
speak. Configure the SDK to send JSON:

- **Java (`aws-sdk-java v1`)**: set `AWS_CBOR_DISABLED=true` or
  `-Dcom.amazonaws.sdk.disableCbor=true`.
- **Java (`aws-sdk-java v2`)**: set `CBOR_ENABLED=false` or
  `-Daws.cborEnabled=false`.
```

- [ ] **Step 2: Correct the WAL retention-map contract**

Replace the contradictory `drop_expired` paragraph with:

```rust
/// Delete closed segments whose records are all past their per-stream
/// retention. The active segment is never dropped. `retentions` must contain
/// every current stream and map it to retention seconds. Retention 0 pins a
/// segment; an absent stream is treated as deleted, so its records no longer
/// pin the segment. Returns how many segments were deleted.
```

- [ ] **Step 3: Document mixed-stream segment collection**

Update `docs/DESIGN.md` to state:

```markdown
A closed segment is dropped only when every stream represented in it is safe:
the stream was deleted, or its newest record in that segment is older than that
stream's finite retention. Retention `0` pins every segment containing that
stream. Segment drops happen only after a successful manifest save.
```

- [ ] **Step 4: Review documentation formatting**

Run:

```bash
git diff --check -- README.md docs/DESIGN.md src/wal.rs
```

Expected: no whitespace errors and no lines over the repository limit in changed prose.

- [ ] **Step 5: Commit documentation corrections**

```bash
git add README.md docs/DESIGN.md src/wal.rs
git commit -m "Correct Java CBOR and WAL retention documentation" \
  -m "Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: End-to-end verification and final review

**Files:**
- Verify all files modified by Tasks 1-5.
- Do not modify or stage `benchmark/`.

**Interfaces:**
- Produces a merge-ready local branch with reproducible evidence and no warnings.

- [ ] **Step 1: Update the Rust toolchain before relying on Clippy parity**

```bash
rustup update stable
```

Expected: latest stable installed; the repository still builds with `rust-version = "1.90"`.

- [ ] **Step 2: Run formatting, strict linting, and all tests**

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

Expected: all commands PASS with zero warnings.

- [ ] **Step 3: Run workflow checks**

```bash
actionlint .github/workflows/*.yml
zizmor .github/workflows/
```

Expected: actionlint emits no output; zizmor reports no findings other than existing suppressions.

- [ ] **Step 4: Run supply-chain checks**

```bash
cargo deny check
```

Expected: PASS for advisories, licenses, bans, and sources. If `cargo-deny` is unavailable, report that explicitly rather than installing it without approval.

- [ ] **Step 5: Rebuild and verify identifiers over HTTP**

Build and start the current binary on a temporary port:

```bash
cargo build
./target/debug/fakestream --port 14567
```

Create stream `S`, then send the following `DescribeStream` bodies:

```json
{"StreamARN":"garbage:stream/S"}
{"StreamName":123,"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/S"}
{"StreamName":"S","StreamARN":123}
```

Expected: each returns HTTP 400 with `ValidationException`. A valid ARN-only request and a matching name/ARN request return HTTP 200.

- [ ] **Step 6: Review the complete diff and repository state**

```bash
git diff pafin/main...HEAD --check
git status --short
git log --format='%h %G? %s' -10
```

Expected:

- no diff whitespace errors;
- only the pre-existing untracked `benchmark/` remains;
- every new commit shows `G` when GPG signing is available.

- [ ] **Step 7: Run independent simplification and review passes**

Invoke `pr-review-toolkit:code-simplifier` on the files changed by Tasks 1-5, then `pr-review-toolkit:code-reviewer` on the final diff. Apply only verified improvements, rerun the affected targeted tests, and do not permit either agent to commit.

- [ ] **Step 8: Report results without pushing**

Summarize:

- commits created;
- tests and tools run with exact outcomes;
- end-to-end HTTP evidence;
- any skipped tool and why;
- confirmation that `benchmark/` was untouched;
- confirmation that nothing was pushed.

# PR #5 Review Fixes Design

## Context

PR #5 fixed four recent review findings, but a read-only re-review found narrower residual cases and two unrelated policy/documentation issues. The goal is to close those findings without undoing the recent fixes or broadening the WAL architecture beyond what the application needs.

## Scope

This change will:

- prevent a buffered NACKed WAL frame from being flushed when a poisoned `Wal` is dropped before another append;
- reject malformed Kinesis stream ARNs and supplied stream identifiers with non-string JSON types;
- split Java SDK v1 and v2 CBOR configuration instructions;
- align Clippy configuration with the governing Rust standards by adopting `tracing` and removing unjustified production `expect()` calls;
- correct adjacent WAL GC documentation while those sections are being updated.

It will not generalize `Wal` over arbitrary writer types, add logging configuration flags, or change unrelated API behavior.

## WAL Writer Lifecycle

`Wal::active` will become `Option<BufWriter<File>>`. The option exists solely to permit safe ownership transfer during roll and destruction; a live `Wal` otherwise maintains the invariant that an active writer is present.

A small helper will return a mutable active writer or an actionable `io::Error` if the invariant is broken. `roll()` will take the old writer and install the new writer before completing bookkeeping:

- for a poisoned segment, `BufWriter::into_parts()` discards buffered bytes without flushing;
- for a healthy segment, `BufWriter::into_inner()` preserves the current normal flush behavior.

`Drop` will take the active writer only when `poisoned` is true and consume it with `into_parts()`. A healthy writer remains in the option and receives normal `BufWriter` drop behavior. This closes the terminal path where no later append triggers `roll()`.

## Stream Identifier Validation

`resolve_stream_name` will distinguish absent fields from present fields of the wrong JSON type. A supplied non-string `StreamName` or `StreamARN` will return `ValidationException`; it will never be treated as absent.

`arn_stream_name` will validate the complete ARN without adding a regex dependency. Accepted ARNs must have:

- the `arn` prefix;
- an AWS partition (`aws` or an `aws-*` partition);
- service `kinesis`;
- a non-empty region;
- a 12-digit account ID;
- exactly one resource component in the form `stream/<name>`.

The extracted name must be non-empty and satisfy the existing stream-name rules. Existing matching-name behavior remains valid, while mismatches continue to return `ValidationException`.

## Logging and Lint Enforcement

Current stable exact versions of `tracing` and `tracing-subscriber` will be added. Startup will initialize a simple formatter without introducing new CLI flags or environment-variable contracts.

Production `println!` and `eprintln!` calls will become severity-appropriate tracing events. Existing `expect()` calls will be handled as follows:

- propagate or explicitly handle failures when they can occur at runtime;
- fail fast with an error event and process abort when a lock is poisoned, preserving the existing stop-on-poison behavior without using panic macros;
- use narrowly scoped, justified Clippy exceptions only for structurally infallible operations such as fixed static headers or serialization into an in-memory buffer.

`Cargo.toml` will set:

- `expect_used = "warn"`;
- `print_stdout = "deny"`;
- `print_stderr = "deny"`.

Strict CI (`-D warnings`) will therefore enforce the policy while permitting only documented local exceptions.

## Documentation

The README will list CBOR controls separately:

- Java SDK v1: `AWS_CBOR_DISABLED=true` or `-Dcom.amazonaws.sdk.disableCbor=true`;
- Java SDK v2: `CBOR_ENABLED=false` or `-Daws.cborEnabled=false`.

The WAL documentation will state that the retention map must contain every current stream and that an absent stream is treated as deleted. The design document will describe mixed-stream segment collection using the per-stream newest timestamp rule.

## Testing

Tests will be written before implementation and shown to fail for the unfixed behavior.

New coverage will include:

- dropping a poisoned WAL with a buffered NACKed frame and no subsequent append;
- rejecting a non-string `StreamName` even when a valid ARN is present;
- rejecting a non-string `StreamARN` even when a valid name is present;
- rejecting substring-only and extra-component ARN forms;
- preserving valid ARN-only, name-only, matching-pair, and mismatch behavior.

Verification after implementation:

1. Run targeted WAL and operation tests.
2. Run `cargo fmt --check`.
3. Run `cargo clippy --all-targets --all-features -- -D warnings`.
4. Run `cargo test --all-features`.
5. Run `actionlint .github/workflows/*.yml`.
6. Run `zizmor .github/workflows/`.
7. Build the current binary and verify malformed/wrong-typed identifiers return HTTP 400 end-to-end.

## Success Criteria

- No code path can flush a buffered frame after that record was reported as failed.
- All supplied stream identifiers are both correctly typed and structurally valid.
- Java users receive generation-correct CBOR instructions.
- CI enforces the governing expect/stdout/stderr policy with only local justified exceptions.
- All existing and new checks pass without warnings.

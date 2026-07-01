# Kinesis local-emulator landscape — research memo

_Scope: replacing LocalStack's Kinesis for local development. Kinesis only — DynamoDB and everything else stays on whatever you run today._

## TL;DR

- **LocalStack's Kinesis provider *is* kinesis-mock.** The default is kinesis-mock's Node.js engine; `KINESIS_MOCK_PROVIDER_ENGINE=scala` (often paired with a multi-GB heap) swaps in the Scala/JVM build for ~10× throughput. So "alternatives to LocalStack for Kinesis" really collapses to: **use kinesis-mock directly, use moto, or build our own.**
- The free-tier pain that started this (LocalStack killed Community on 2026-03-23) is **not** a Kinesis-capability gap — kinesis-mock and moto both cover the operations we use. The real wins from building our own are **memory footprint** and **zero JVM/Python runtime**, not missing features.
- The actual usage is tiny and polling-only. There's **no** enhanced fan-out, resharding, encryption, tags, or KCL. That makes a from-scratch Rust server genuinely small (it's already built — see `DESIGN.md`).

## Operations actually used

The target setup is a polling consumer (Node.js, `aws-sdk-js v3`) and a producer (Python, `boto3`). The complete operation surface:

| Operation | Caller | Notes |
|---|---|---|
| `CreateStream` | stream init script | shard-count 1 |
| `ListStreams` | producer | |
| `DescribeStream` | integration test | |
| `ListShards` | consumer `shardDiscovery()` | single page, no pagination |
| `GetShardIterator` | consumer | `TRIM_HORIZON` + `AFTER_SEQUENCE_NUMBER` |
| `GetRecords` | consumer poller | `Limit` up to 10000; reads `NextShardIterator`, `MillisBehindLatest` |
| `PutRecord` | producer | binary payload, `PartitionKey` from the entity id |

Error paths the code depends on: `ExpiredIteratorException` (poller restarts the iterator) and `ResourceNotFoundException`. The consumer assumes **a single shard** and does its own checkpointing in **DynamoDB** (not Kinesis). Enhanced fan-out / `SubscribeToShard`, `MergeShards`/`SplitShard`, and KCL coordination are **not used**.

> Implication: resharding, enhanced fan-out, and KCL are a *future* differentiator, not what's needed to be a drop-in. The MVP only needs the 7 ops above + `PutRecords`/`DeleteStream`/`DescribeStreamSummary` as cheap niceties.

## The alternatives

### 1. kinesis-mock (`etspaceman/kinesis-mock`)
- **Language/runtime:** Scala on JVM; also ships a Node.js build (`npm i kinesis-local`) and a Docker image (`ghcr.io/etspaceman/kinesis-mock`).
- **Activity:** Healthy — v0.6.2, 1,300+ commits.
- **Coverage:** Broad and correct (it's what LocalStack embeds). Polling consumers only for KCL — no enhanced fan-out, same as us.
- **Persistence:** `SHOULD_PERSIST_DATA`, `PERSIST_INTERVAL`, `PERSIST_PATH`.
- **Cost for us:** JVM memory. LocalStack's Scala engine defaults to 256m initial / 512m max heap and is documented to need *more* under load (4 GB is common). The Node engine avoids the JVM but is the slower path and still carries a Node runtime. There's a known LocalStack OOM/CPU bug for the Kinesis mock ([localstack#9755](https://github.com/localstack/localstack/issues/9755)).
- **Verdict:** The most correct option off the shelf. If we don't build our own, **run kinesis-mock directly** (drop LocalStack) — either the Docker image or `kinesis-local`. The reason to build our own is purely footprint/startup.

### 2. moto (`getmoto/moto`)
- **Language/runtime:** Python; standalone via `moto_server`.
- **Coverage:** 26/34 Kinesis ops — `CreateStream`, `DeleteStream`, `DescribeStream`, `ListStreams`, `PutRecord(s)`, `GetRecords`, `GetShardIterator`, `ListShards`, `MergeShards`/`SplitShard`, consumer registration. **No `SubscribeToShard`** (fine — we poll).
- **Limitations:** Returns fixed limits, no account-specific settings, some pagination gaps. Primarily a *test mock*, not a long-running dev server; behavioral fidelity (errors/edge cases) is shallower than kinesis-mock.
- **Verdict:** Easiest for Python unit tests; covers every op we use. Weaker as a persistent local dev backend, and carries a Python runtime.

### 3. Also-rans
- **ministack** — Python, ~3 weeks old at evaluation; devs admit Kinesis doesn't mimic exceptions/edge cases/rate limits.
- **floci** — Java; Kinesis marked *partial*; we want to avoid Java.
- **robotocore** — thin wrapper over moto/boto (inherits moto's behavior).
- **local-web-services** — no Kinesis.

## Recommendation

For the goal (drop-in local Kinesis, minimal memory, Rust, no runtime):

1. **Build it** — the surface is small and the footprint win is real. Status: **done, MVP works** (`DESIGN.md`, `../README.md`). Idle RSS **~2 MB**, ~12 MB holding 20k×200 B records, 493 KB binary, instant start — vs a 512 MB–4 GB JVM heap.
2. **Fallback if we drop the build:** run **kinesis-mock directly** (Docker image or `kinesis-local`) and delete LocalStack — same engine, no LocalStack wrapper. Use **moto** for Python unit tests.

Sources: [LocalStack Kinesis docs](https://docs.localstack.cloud/aws/services/kinesis/) · [LocalStack 4.5 release notes](https://blog.localstack.cloud/localstack-release-v-4-5-0/) · [localstack#9755 (Kinesis mock OOM)](https://github.com/localstack/localstack/issues/9755) · [kinesis-mock](https://github.com/etspaceman/kinesis-mock) · [moto Kinesis](https://docs.getmoto.org/en/latest/docs/services/kinesis.html)

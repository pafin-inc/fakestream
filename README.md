# fakestream

A minimal, low-memory local **Amazon Kinesis Data Streams** emulator, written in Rust.
Point any AWS SDK at it with dummy credentials and it behaves like Kinesis for the
PutRecord + polling-consumer workflows. Kinesis only — nothing else.

- **~2 MB idle RSS**, ~12 MB holding 20k records. No JVM, no Python, no async runtime.
- **493 KB** release binary, instant startup.
- Drop-in for a LocalStack-based local stack (replaces LocalStack's Kinesis;
  DynamoDB and friends stay exactly where they are).

See `docs/RESEARCH.md` for the alternatives comparison, `docs/DESIGN.md` for the design, and
`docs/WAL.md` for the write-ahead log's crash-consistency design.

## Build & run

```sh
cargo build --release
./target/release/fakestream            # listens on 0.0.0.0:4567, in-memory only
./target/release/fakestream --port 4567
FAKESTREAM_PORT=4567 ./target/release/fakestream
```

### Configuration

Every option takes a CLI flag or an environment variable (env wins).

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--port` | `FAKESTREAM_PORT` | `4567` | Listen port |
| `--persist <dir>` | `FAKESTREAM_PERSIST_PATH` | _(off)_ | Enable disk persistence; value is a **directory** |
| `--persist-interval <secs>` | `FAKESTREAM_PERSIST_INTERVAL` | `5` | Maintenance cadence (trim + segment drop + manifest save) |
| `--segment-bytes <n>` | `FAKESTREAM_SEGMENT_BYTES` | `67108864` (64 MiB) | WAL segment roll size |
| `--retention-hours <n>` | `FAKESTREAM_RETENTION_HOURS` | `24` | Default record TTL (Kinesis retention) |
| `--ttl-seconds <n>` | `FAKESTREAM_TTL_SECONDS` | — | Precise TTL override (wins over hours; `0` = keep forever) |

### Persistence & TTL

With `--persist <dir>`, fakestream persists state to a **directory** and reloads it on startup —
so **unconsumed records survive a restart**. Persistence is opt-in; the default is pure
in-memory (ideal for tests).

The persistence layout inside `<dir>`:
- `manifest.json` — stream definitions and the sequence counter high-water mark (atomically
  rewritten on stream create/delete and on each maintenance tick; records are **not** inlined).
- `wal/seg-NNNNNNNNNN.log` — segmented append-only write-ahead log (length-framed postcard
  records). On startup, the manifest is loaded and then all WAL segments are replayed in order
  to recover records. A crash-torn trailing frame is detected and truncated so appends stay
  clean. Whole segments are dropped once every record in them is past retention.

The maintenance thread (every `--persist-interval` seconds) trims expired records in memory,
saves the manifest (preserving the current seq high-water), and drops fully-expired WAL
segments. A hard crash loses at most the last interval's records.

"Unconsumed" needs no server-side tracking: the consumers checkpoint in DynamoDB and resume via
`AFTER_SEQUENCE_NUMBER`, so after a restart already-consumed records are skipped and only
unconsumed ones are re-read. Preserving the sequence counter is what makes this correct.

Records older than their stream's retention are trimmed in memory and excluded from WAL replay.
A stream's retention comes from `CreateStream`'s `RetentionPeriodHours`, or the configured
default when omitted. Set `--ttl-seconds 0` to keep records forever.

```sh
# dev: persist to disk, keep unconsumed records 1h
./target/release/fakestream --persist ./fakestream-data/ --retention-hours 1
```

## Try it

```sh
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=local AWS_PAGER=""
EP="--endpoint-url=http://localhost:4567"

aws $EP kinesis create-stream --stream-name my-stream --shard-count 1
aws $EP kinesis put-record --stream-name my-stream \
    --partition-key key-1 --cli-binary-format raw-in-base64-out --data "hello"

IT=$(aws $EP kinesis get-shard-iterator --stream-name my-stream \
    --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON \
    --query ShardIterator --output text)
aws $EP kinesis get-records --shard-iterator "$IT"
```

## Supported operations

`CreateStream`, `DeleteStream`, `ListStreams`, `DescribeStream`, `DescribeStreamSummary`,
`ListShards`, `GetShardIterator` (`TRIM_HORIZON` / `LATEST` / `AT`/`AFTER_SEQUENCE_NUMBER` /
`AT_TIMESTAMP`), `PutRecord`, `PutRecords`, `GetRecords`.

Errors: `ResourceNotFoundException`, `ResourceInUseException`, `ExpiredIteratorException`
(5-minute iterator TTL), `ValidationException`, `InvalidArgumentException`, `InternalFailure`.

## Docker

Pull the prebuilt multi-arch image (`linux/amd64`, `linux/arm64`) from GHCR:

```sh
docker pull ghcr.io/pafin-inc/fakestream:latest
docker run --rm -p 4567:4567 ghcr.io/pafin-inc/fakestream:latest
```

Or build it locally:

```sh
docker build -t fakestream .
docker run --rm -p 4567:4567 fakestream
```

## Drop-in for LocalStack Kinesis

A common setup runs LocalStack only for Kinesis, while DynamoDB (consumer checkpoints) and other
services run elsewhere. fakestream replaces just the **Kinesis** endpoint — keep your existing
DynamoDB (LocalStack with `SERVICES=dynamodb`, or `amazon/dynamodb-local`).

### docker-compose

Narrow LocalStack to `SERVICES=dynamodb` and add a `fakestream` service:

```yaml
  fakestream:
    image: ghcr.io/pafin-inc/fakestream    # or build: ./fakestream
    ports:
      - "4567:4567"
    environment:
      - FAKESTREAM_PERSIST_PATH=/data/fakestream-data/   # survive restarts
      - FAKESTREAM_RETENTION_HOURS=24
    volumes:
      - fakestream-data:/data

  localstack:
    image: localstack/localstack:4.14
    ports:
      - "4566:4566"
    environment:
      - SERVICES=dynamodb           # Kinesis now served by fakestream

volumes:
  fakestream-data:
```

### Stream init

```sh
aws --endpoint-url=http://localhost:4567 kinesis create-stream --shard-count 1 --stream-name my-stream
```

### Point your clients at it

Override only the **Kinesis** endpoint, leaving DynamoDB and other services untouched:

- **Python producer (`boto3`)**: set `AWS_ENDPOINT_URL_KINESIS=http://localhost:4567` (service-specific).
- **JS consumer (`aws-sdk-js v3`)**: set the Kinesis client `endpoint` to `http://localhost:4567`;
  leave the DynamoDB endpoint where it is.
Java SDK clients default to CBOR (`application/x-amz-cbor-1.1`), which fakestream does not
speak. Configure the SDK to send JSON:

- **Java (`aws-sdk-java v1`)**: set `AWS_CBOR_DISABLED=true` or
  `-Dcom.amazonaws.sdk.disableCbor=true`.
- **Java (`aws-sdk-java v2`)**: set `CBOR_ENABLED=false` or `-Daws.cborEnabled=false`.

> Note: single-shard streams match the common single-shard consumer assumption. fakestream supports
> `--shard-count N`, but a consumer that only reads the first `ListShards` page sees one shard.

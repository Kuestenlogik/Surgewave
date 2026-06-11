# ADR-014: Disaggregated Compute/Storage — Two Modes

| Status   | Date       |
|----------|------------|
| Proposed | 2026-06-11 |

## Context

Today every Surgewave topic uses the same write path: producer → broker leader → ISR replication to N-1 followers → local segment files → optional cold-tier offload to S3 via `Storage.Tiering`. The `Storage.Engine.S3` exists but is used as a primary-storage *backend* — the broker still owns the hot path, the data still flows broker-to-broker over the network for replication, and the durability story is "trust the replicas".

That layout is the right default for low-latency in-cluster workloads. It is the wrong default for two newer workload shapes that are now part of Surgewave's positioning:

1. **Cloud-cost-sensitive workloads.** Cross-AZ replication traffic dominates the bill on AWS / Azure. A 3× replication-factor topic costs roughly 3× the egress + 3× the storage of the same topic stored once on S3. Operators running analytical or audit-log pipelines do not need the latency that justifies that cost.
2. **Disaggregated cloud-native deployments.** The wider broker market (WarpStream, AutoMQ, Confluent Kora, Bufstream) has converged on a pattern where the broker becomes a coordination/metadata layer and the bytes live on object storage. The durability story shifts from "ISR replicas" to "S3 already replicates internally (11×9 durability)".

Surgewave needs to support these workloads without losing the sub-10 ms produce-latency story it already has on the classical path. A single "disaggregated" mode cannot meet both targets — there are two distinct sweet spots and they have incompatible latency profiles:

- **WAL + S3-offload (AutoMQ-style).** Broker keeps a local WAL on EBS/NVMe so produce-ack stays sub-10 ms; a background job compacts the WAL into S3 stream objects on a short interval (seconds). No ISR. S3 is the durable store after offload. Embedded-friendly because the WAL works locally even without S3 configured.
- **Stateless agents + S3-direct (WarpStream-style).** Producer hits a stateless agent process. Agent buffers the batch in RAM, periodically PUTs to S3, then commits the offset range to a metadata cell. No WAL, no per-partition leader, no embedded mode. Produce-P99 is dominated by the S3 PUT (~400-600 ms). Horizontal scaling is trivial because agents are interchangeable.

Both modes share the same two coordination requirements: a per-partition manifest mapping `[offset_lo, offset_hi]` to object-store keys, and an offset-commit protocol that admits new manifest entries atomically.

## Decision

Surgewave introduces **two parallel disaggregated storage modes**, both selected via a per-topic config property `storage.mode`. The existing replicated path stays the default and is unchanged for topics that do not opt in.

| `storage.mode`             | Engine                                                 | Latency target | Durability source |
|----------------------------|--------------------------------------------------------|----------------|-------------------|
| `replicated` *(default)*   | Existing local-segment + ISR path                      | Sub-10 ms P99  | ISR replicas      |
| `disaggregated-wal`        | Local WAL → background flush to S3 stream objects      | Sub-10 ms P99  | S3 after offload  |
| `disaggregated-stateless`  | Stateless agent → RAM buffer → direct S3 PUT           | ~400-600 ms P99 | S3 directly       |

### Concrete consequences

1. **Topic-level config.** `storage.mode` is added as a topic property in `TopicConfig`. Validation rejects unknown values and combinations the broker cannot satisfy in the current cluster (e.g. `disaggregated-*` without a configured object-store endpoint). Default for new topics is `replicated`. The property is *not* alterable mid-life in v1: a topic chooses its mode at create time. (Alter-config across modes ships in a later iteration once segment-migration is built.)

2. **Per-partition manifest.** Both disaggregated modes maintain a manifest per `TopicPartition` consisting of an ordered list of `StreamObjectRef { ObjectKey, FirstOffset, LastOffset, BytesOnDisk, CreatedAt }`. The manifest persists in the existing cluster-metadata store (`Clustering` package, KRaft-backed today). No separate metadata service in v1 — that is a scale follow-up.

3. **No ISR for disaggregated topics.** Topics in `disaggregated-*` mode have `replication.factor = 1` enforced at create-time. The broker still tracks a single owning broker for coordination (offset assignment, consumer-group coordination), but partition-level replicas are explicitly forbidden — durability comes from S3, replicating again wastes money. Consumer-group state stays on the broker the same way it does today.

4. **Wire-protocol behaviour.** The decision is asymmetric by client capability:
   - **Native protocol clients** receive a per-topic "produce strategy" hint inside the `MetadataResponse`. Three strategies: `replicated` (existing path, ack via leader), `wal-via-broker` (acks-via-leader, broker writes WAL + schedules flush), `stateless-direct` (broker hands out a presigned PUT URL per batch, client uploads, commits via a follow-up `CommitWriteRequest`).
   - **Kafka wire clients** always look like normal Kafka producers from the outside — they cannot consume presigned URLs and we cannot extend the wire protocol. The broker accepts Produce requests for `disaggregated-stateless` topics, buffers them in-process (the broker becomes a relay agent for these clients), and follows the same S3-PUT → commit path. Cost-win is preserved (no ISR); the latency profile of `disaggregated-stateless` over Kafka-wire is identical to the native-direct path because both end up doing one S3 PUT per buffered batch.

5. **Failure modes.** Documented in the user-facing operations guide:
   - `disaggregated-wal`: a WAL-mode broker crashing before its scheduled S3 flush loses no data, because WAL recovery on restart re-flushes any pending stream object. A WAL-mode broker losing its *disk* loses the un-flushed window (typically seconds). Operators who cannot accept that window must keep `replicated` topics.
   - `disaggregated-stateless`: an agent crashing with un-PUT batches loses those batches. `acks=all` semantics means the client only sees the ack after the S3 PUT returned 200; so a crashed agent never reports false durability. Producers retry as normal.
   - Object store unreachable: produce blocks for `disaggregated-*` topics, succeeds normally for `replicated` topics. The broker surfaces this as `STORAGE_UNAVAILABLE` instead of stalling silently.

6. **Embedded-mode story.**
   - `replicated`: unchanged, in-process broker.
   - `disaggregated-wal`: supported. WAL is the embedded story by default; S3 offload becomes a no-op when no object store is configured. Embedded topic can be promoted later by simply attaching a bucket.
   - `disaggregated-stateless`: **not supported in embedded mode** — an in-process broker with no S3 reachable cannot satisfy the contract. Embedded `Build()` rejects this mode at startup with a clear error message.

7. **Object-store abstraction.** `Storage.Tiering.IRemoteStorageProvider` is the contract both modes use for S3-equivalent backends. The S3-AWS provider already exists; an Azure-Blob + a GCS provider land as separate ADRs/PRs (out of G21's scope but consume the same interface).

### What stays out of v1

- **Compacted disaggregated topics.** Log compaction in disaggregated mode requires rewriting stream objects, which is doable but adds significant complexity. v1 supports retention-based deletion only; producers using compaction get a config-validation error if they try to combine it with `disaggregated-*`.
- **Cross-topic transactions on disaggregated topics.** Producer transactions stay restricted to `replicated` topics in v1. A transaction touching a `disaggregated-*` topic is rejected at the coordinator with `INVALID_TXN_STATE`. Lifting this requires a two-phase-commit story over S3 PUTs that is its own ADR.
- **Mid-life alter-config across modes.** Once a topic is created with a mode, that mode is fixed for v1. Switching means recreate-with-mirror.

## Alternatives Considered

- **Single disaggregated mode (one or the other).**
  - WAL-only: keeps sub-10 ms latency, but operators with cost-first batch workloads pay for EBS volumes they do not need. Surrenders the "WarpStream-cheap" use case.
  - Stateless-only: maximum cost-win, but Surgewave's sub-10 ms latency story dies — embedded mode becomes impossible, and the broker positioning shifts to a slower batch-broker. Rejected.
- **`storage.mode` as a cluster-wide setting instead of per-topic.** Simpler in code but defeats the actual operator need: different workloads on the same cluster want different tradeoffs. Per-topic is the conventional path (Confluent, AutoMQ, WarpStream all gate at topic-level).
- **External metadata service from day one (WarpStream "cells").** Would let many stateless agents share a horizontally-scaled metadata store. We don't need that scale yet — the existing broker metadata handles it for ≥100k partitions per broker in our benchmarks. Postponed; the manifest design is forward-compatible.
- **Reuse `Storage.Engine.S3` as the disaggregated backend directly.** That engine writes one S3 object per *segment-flush*, designed for the broker-owned hot path. The disaggregated modes need a different write rhythm (WAL-flush batches, stateless agent buffer-flushes) and a different read path (manifest-driven, not segment-driven). Sharing the low-level S3 client + buffer pool is appropriate; sharing the `ISurgewaveStorageEngine` implementation would force-fit two different write contracts. Disaggregated gets a new `StreamObjectStore` abstraction layered on top of the existing remote-provider.

## Implementation Plan

Each phase ships its own commits and is independently testable. The order matters: configuration plumbing must land before any write-path change.

- **P0 — ADR (this document).** Pin the architectural decisions for review.
- **P1 — Topic-config + metadata-layer skeleton.** `storage.mode` as a topic property, schema-validated, persisted in cluster metadata, surfaced in Control UI. `StreamObjectRef` + `PartitionManifest` types under a new `Kuestenlogik.Surgewave.Storage.Disaggregated` project. No write-path change yet.
- **P2 — `disaggregated-wal` mode.** WAL writer reuses the existing `FileLogSegment`; a `WalFlusher` periodically reads sealed segments, packs them into a `StreamObject` (a concatenated, indexed S3 PUT payload), uploads via `IRemoteStorageProvider`, and appends the resulting `StreamObjectRef` to the partition manifest. Read path becomes: serve from WAL until the requested offset is flushed, then serve from the manifest's stream objects.
- **P3 — `disaggregated-stateless` mode.** Agent-style write path: incoming Produce batches buffer in a `StatelessAgentBuffer`, the buffer flushes on size/time threshold by PUTting to S3 and atomically appending to the manifest. No WAL. Reads are manifest-driven only.
- **P4 — Native-client integration.** `SurgewaveNativeClient` learns the three produce strategies via `MetadataResponse`; the client picks the right one per partition. Pre-existing `replicated` clients see no change.
- **P5 — Kafka-wire relay fallback.** Broker accepts `Produce` for disaggregated topics from classical Kafka clients, runs the same buffer→PUT→commit flow internally. From the client's POV the API is unchanged.
- **P6 — Tests + cost-bench + docs.** Integration tests for each mode (failure injection: agent crash, S3 timeout, mid-flush kill), a cost-comparison run in the public benchmark suite (`disaggregated-*` vs. `replicated` over identical workload, with $/GB-month + cross-AZ-bytes numbers), `docs/storage/disaggregated.md` operator guide.

## Consequences

- Operators get an explicit cost/latency knob at topic granularity. The default behaviour (sub-10 ms latency, full ISR durability) does not change for any existing deployment.
- The broker grows two new internal write paths. The classical path stays the simplest and fastest; disaggregated paths add complexity but are isolated behind the `storage.mode` switch.
- Surgewave's positioning explicitly covers three workload archetypes after this lands: real-time (replicated), real-time-cheap (disaggregated-wal), batch-cheap (disaggregated-stateless). Documentation must guide operators to the right knob, not present three equivalent options.
- Future enterprise extensions (replication geo-fence, schema-aware compaction) need to grow disaggregated-aware variants. v1 enforces "disaggregated topics participate in fewer enterprise features"; that boundary should narrow over later releases, not widen.

## See also

- [G21 — Disaggregated compute/storage mode](https://github.com/Kuestenlogik/Surgewave/issues/11) — the roadmap entry this ADR implements.
- ADR-001 — Pluggable Storage Engine Abstraction (the `ISurgewaveStorageEngine` contract this builds *alongside*, not *on top of*).
- ADR-012 — Zero-Copy & High-Performance Patterns (the WAL flusher's stream-object packer must keep the existing zero-copy invariants on the read path).
- `Kuestenlogik.Surgewave.Storage.Tiering.IRemoteStorageProvider` — object-store contract reused by both new modes.

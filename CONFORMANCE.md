# Surgewave — Kafka Conformance Status

Surgewave aims to be a drop-in replacement for Apache Kafka. This document is the
authoritative, externally-verifiable statement of what's in scope, what's
implemented, what's stubbed, and what is intentionally out of scope. It is
updated alongside the wire-protocol code and tracked in CI.

If you're integrating a Kafka client against Surgewave, this is the page that tells
you whether your client's path is on the wired, advertised-only, or
unsupported side of the matrix — without having to read source.

**Headline numbers**

- **Kafka 4.0 wire-protocol RPCs implemented**: 56 / ~55 (the five EOS transaction RPCs were re-classified from "gRPC-only" to "wired" after the G16 follow-up traced the `SurgewaveBroker.ProcessRequestAsync` fast-path)
- **Kafka 4.2 wire-protocol RPCs advertised**: 60 of 93 enum entries
- **Wired with full handler logic**: 72 RPCs (added ElectLeaders, AlterPartitionReassignments, ListPartitionReassignments, DescribeUserScramCredentials, AlterUserScramCredentials)
- **Advertised but not wired (will return UnsupportedApiKey on the wire)**: 0 — every advertised admin RPC now has a handler
- **Major KIPs implemented**: 15 (KIP-98, KIP-516, KIP-595, KIP-714 stub, KIP-848, KIP-853, KIP-892, KIP-894, KIP-903, KIP-932, KIP-936, KIP-985, KIP-994, KIP-1071, KIP-895)
- **Confluent Schema Registry**: wire format + REST API audited compatible (see [Confluent Schema Registry compatibility](#confluent-schema-registry-compatibility) section below)
- **Cross-client tested**: Confluent.Kafka 2.x (.NET), librdkafka 2.14 (CGv2 e2e), Confluent.SchemaRegistry contract-pinned

**Source of truth**

- API version ranges advertised to clients:
  [`src/Kuestenlogik.Surgewave.Protocol.Kafka/Requests/ApiVersionsRequest.cs`](src/Kuestenlogik.Surgewave.Protocol.Kafka/Requests/ApiVersionsRequest.cs#L239-L367)
- Wired handler set:
  [`src/Kuestenlogik.Surgewave.Broker/Program.cs`](src/Kuestenlogik.Surgewave.Broker/Program.cs#L1144-L1162)
- Per-handler `SupportedApiKeys`:
  [`src/Kuestenlogik.Surgewave.Broker/Handlers/`](src/Kuestenlogik.Surgewave.Broker/Handlers/)

---

## Status legend

| Status | Meaning |
|--------|---------|
| **Wired** | A real handler processes the request and returns a meaningful response. Round-trip-tested against at least one Kafka client. |
| **Stub** | Handler exists; accepts the request and returns a well-formed but degenerate response (e.g. empty subscription set). Clients see success and continue. Documented per row. |
| **gRPC-only** | Functionality lives in `Kuestenlogik.Surgewave.Api.Grpc`; no Kafka wire handler is registered. A Kafka-wire client will see `UNSUPPORTED_VERSION (35)`. |
| **Advertised, not wired** | `ApiVersions` lists the key, but no handler is registered. A client that calls it will see `UNSUPPORTED_VERSION`. We advertise these so admin clients don't crash on `ApiVersions` parsing. |
| **Not implemented** | No handler, not advertised. Hard `UnsupportedApiKey` response if a client somehow calls it. |

---

## Kafka 4.x API matrix

Versions are the range Surgewave advertises. "Kafka" is the latest range published
by Apache Kafka 4.2 — they match unless noted.

### Core data plane

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 0  | Produce            | 0–13 | Wired | `DataApiHandler` | Quotas, dedup, deferred delivery, TTL. MinVersion 0 (not 3) is intentional for librdkafka compression detection. |
| 1  | Fetch              | 4–18 | Wired | `DataApiHandler` | Zero-copy, tiered storage hand-off, Topic IDs (KIP-516), incremental fetch sessions. |
| 2  | ListOffsets        | 1–11 | Wired | `DataApiHandler` | Timestamp + offset lookup, leader-epoch aware. |
| 3  | Metadata           | 0–13 | Wired | `MetadataApiHandler` | Auto-topic-create hook, broker discovery. |
| 18 | ApiVersions        | 0–5  | Wired | `MetadataApiHandler` | Advertises `group.version`, `transaction.version`, `metadata.version` features. |
| 60 | DescribeCluster    | 0–2  | Wired | `MetadataApiHandler` | KIP-700. |

### Consumer Group classic (v1)

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 8  | OffsetCommit     | 2–10 | Wired | `ConsumerGroupApiHandler` | Persisted via `OffsetStore`. |
| 9  | OffsetFetch      | 1–10 | Wired | `ConsumerGroupApiHandler` | |
| 10 | FindCoordinator  | 0–6  | Wired | `MetadataApiHandler` | Routes to group/transaction coordinator. |
| 11 | JoinGroup        | 0–9  | Wired | `ConsumerGroupApiHandler` | |
| 12 | Heartbeat        | 0–4  | Wired | `ConsumerGroupApiHandler` | |
| 13 | LeaveGroup       | 0–5  | Wired | `ConsumerGroupApiHandler` | |
| 14 | SyncGroup        | 0–5  | Wired | `ConsumerGroupApiHandler` | |
| 15 | DescribeGroups   | 0–6  | Wired | `ConsumerGroupApiHandler` | |
| 16 | ListGroups       | 0–5  | Wired | `ConsumerGroupApiHandler` | |
| 42 | DeleteGroups     | 0–2  | Wired | `ConsumerGroupApiHandler` | |
| 47 | OffsetDelete     | 0    | Wired | `ConsumerGroupApiHandler` | KIP-496. Conservative reading: a group with active members rejects the delete with `GROUP_SUBSCRIBED_TO_TOPIC` for every requested partition (Surgewave doesn't decode the classic-protocol Subscription bytes, so it can't ask "is THIS topic subscribed" — empty groups always allow the delete). 5 tests. |

### Consumer Group v2 (KIP-848)

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 68 | ConsumerGroupHeartbeat | 0–1 | Wired | `ConsumerGroupV2ApiHandler` | Server-side assignment, no SyncGroup round-trip. `ConsumerGroupV2Reconciler` + `ConsumerGroupV2Coordinator` ship full state machine; persistence verified by `ConsumerGroupV2PersistenceTests`. |
| 69 | ConsumerGroupDescribe  | 0–1 | Wired | `ConsumerGroupV2ApiHandler` | |

### Share Groups (KIP-932)

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 76 | ShareGroupHeartbeat        | 1   | Wired | `ShareGroupApiHandler` | Queue-style consumer protocol. |
| 77 | ShareGroupDescribe         | 1   | Wired | `ShareGroupApiHandler` | |
| 78 | ShareFetch                 | 1–2 | Wired | `ShareGroupApiHandler` | Visibility timeout, redelivery counter. |
| 79 | ShareAcknowledge           | 1–2 | Wired | `ShareGroupApiHandler` | Ack/Nack/Renew. |
| 90 | DescribeShareGroupOffsets  | 0–1 | Wired | `ShareGroupApiHandler` | |
| 91 | AlterShareGroupOffsets     | 0   | Wired | `ShareGroupApiHandler` | |
| 92 | DeleteShareGroupOffsets    | 0   | Wired | `ShareGroupApiHandler` | |
| 83 | InitializeShareGroupState  | —   | Not implemented | — | Internal state APIs not exposed; share-group state lives inside the coordinator. |
| 84 | ReadShareGroupState        | —   | Not implemented | — | |
| 85 | WriteShareGroupState       | —   | Not implemented | — | |
| 86 | DeleteShareGroupState      | —   | Not implemented | — | |
| 87 | ReadShareGroupStateSummary | —   | Not implemented | — | |

### Streams Groups (KIP-1071)

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 88 | StreamsGroupHeartbeat | 0 | Wired | `StreamsGroupApiHandler` | Topology-aware sticky assignment. |
| 89 | StreamsGroupDescribe  | 0 | Wired | `StreamsGroupApiHandler` | |

### Transactions (KIP-98 / KIP-892 / KIP-994)

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 22 | InitProducerId       | 0–6 | Wired | `SurgewaveBroker.ProcessRequestAsync` → `TransactionCoordinator.HandleInitProducerIdAsync` | Fast-path switch dispatches transaction RPCs ahead of the `RequestDispatcher`. KIP-892 epoch fencing enforced (`Kip892TransactionDefenseTests`). |
| 24 | AddPartitionsToTxn   | 0–5 | Wired | same → `HandleAddPartitionsToTxn` | |
| 25 | AddOffsetsToTxn      | 0–4 | Wired | same → `HandleAddOffsetsToTxn` | Exercised by `TransactionTests.ConsumeProcessProduce_AtomicallyCommitsOffsetsAndOutputs`. |
| 26 | EndTxn               | 0–5 | Wired | same → `HandleEndTxnAsync` | Writes commit/abort markers to all participating partitions. |
| 27 | WriteTxnMarkers      | 0–1 | Wired | `InterBrokerApiHandler` | Inter-broker only. |
| 28 | TxnOffsetCommit      | 0–5 | Wired | same → `HandleTxnOffsetCommit` | Exercised by `TransactionTests.ConsumeProcessProduce_AtomicallyCommitsOffsetsAndOutputs`. |
| 65 | DescribeTransactions | 0   | Wired | `SurgewaveBroker.ProcessRequestAsync` → `TransactionCoordinator.HandleDescribeTransactions` | Maps the existing per-id projection onto the on-the-wire `TransactionState` shape; unknown ids surface as error rows rather than being dropped. 2 wire tests. |
| 66 | ListTransactions     | 0–2 | Wired | same → `HandleListTransactions` | Threads `StateFilters` / `ProducerIdFilters` / KIP-994 `DurationFilter` / KIP-1152 `TransactionalIdPattern` straight through to the existing `ListTransactions` semantic path; the pathological-regex DoS defence (`Kip994ListTransactionsFiltersTests`) applies here too. 3 wire tests. |
| 67 | AllocateProducerIds  | 0–1 | Wired (controller-side) | — | Producer ID pool inside `ProducerStateManager`. |

> **Confluent.Kafka 2.x transactional clients (`InitTransactions` / `BeginTransaction` /
> `ProduceAsync` / `SendOffsetsToTransaction` / `CommitTransaction`) round-trip green
> against an embedded Surgewave broker over the Kafka wire — see the six tests in
> `tests/Kuestenlogik.Surgewave.IntegrationTests/TransactionTests.cs` (commit, abort, isolation
> levels, mixed transactions, producer fencing, consume-process-produce EOS).
> The earlier "wire-binding gap" line in this document was incorrect — the audit
> missed the fast-path switch in `SurgewaveBroker.ProcessRequestAsync` (lines 540-550)
> that dispatches transaction RPCs ahead of the `RequestDispatcher` table.

### Topic & partition admin

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 19 | CreateTopics       | 2–7 | Wired | `TopicAdminHandler` | |
| 20 | DeleteTopics       | 1–6 | Wired | `TopicAdminHandler` | |
| 21 | DeleteRecords      | 0–2 | Wired | `TopicAdminHandler` | Truncate at offset. |
| 37 | CreatePartitions   | 0–3 | Wired | `TopicAdminHandler` | |
| 75 | DescribeTopicPartitions | 0 | Wired | `MetadataApiHandler` | KIP-966 paginated metadata fetch — used by Java Kafka client 4.0+ to walk large clusters in bounded chunks. Honours `ResponsePartitionLimit` and `StartingCursor`; emits `NextCursor` when truncated. Unknown topics surface with `UnknownTopicOrPartition` rather than being dropped. Internal topics (`_*` prefix) flagged via `IsInternal`. 5 tests. |
| 43 | ElectLeaders            | 0–2 | Wired | `ClusterAdminHandler` → `ClusterController.ElectLeaderAsync` | KIP-460. Per-partition: prefers an ISR member, falls back to first ISR, then unclean (any alive replica) when ISR is empty. Top-level error = `NotController` when this broker isn't the cluster controller. |
| 45 | AlterPartitionReassignments | 0–1 | Wired | `ClusterAdminHandler` → `PartitionReassignmentManager.ExecuteReassignmentAsync` | KIP-455. Builds a `ReassignmentPlan` from the request and hands it to the manager's 4-state machine (Pending → Adding → Syncing → Completing → Completed). Cancel-via-empty-Replicas surfaces as `InvalidRequest` per partition — the manager doesn't expose a cancel API yet. |
| 46 | ListPartitionReassignments  | 0   | Wired | `ClusterAdminHandler` → `PartitionReassignmentManager.GetActiveReassignments` | Filters by topic if specified; returns active state with `TargetReplicas` / `AddingReplicas` / `RemovingReplicas` per the wire shape. |

### Configuration

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 32 | DescribeConfigs            | 1–4 | Wired | `ConfigApiHandler` | |
| 33 | AlterConfigs               | 0–2 | Wired | `ConfigApiHandler` | |
| 44 | IncrementalAlterConfigs    | 0–1 | Wired | `ConfigApiHandler` | |
| 74 | ListConfigResources        | 0–1 | Wired | `TelemetryApiHandler` | KIP-1106. Returns the configured `ClientTelemetryConfig.RequestedMetrics` as `ConfigResource` rows when telemetry is enabled; empty list otherwise. 2 tests. |

### Security

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 17 | SaslHandshake     | 0–1 | Wired | `SecurityApiHandler` | PLAIN, SCRAM-SHA-256/512, and OAUTHBEARER (KIP-936). OAUTHBEARER stands up when `Surgewave:Security:OAuthBearer:Enabled=true` and `OAUTHBEARER` is in `SaslMechanisms` — JWT validation against `OidcAuthority` (well-known discovery) or `JwksUri`, configurable issuer + audience + principal claim. |
| 36 | SaslAuthenticate  | 0–2 | Wired | `SecurityApiHandler` | |
| 29 | DescribeAcls      | 1–3 | Wired | `SecurityApiHandler` | Literal/Prefix/Suffix/Regex resource patterns. |
| 30 | CreateAcls        | 1–3 | Wired | `SecurityApiHandler` | |
| 31 | DeleteAcls        | 1–3 | Wired | `SecurityApiHandler` | |
| 38 | CreateDelegationToken    | 1–3 | Wired | `DelegationTokenApiHandler` | |
| 39 | RenewDelegationToken     | 1–2 | Wired | `DelegationTokenApiHandler` | |
| 40 | ExpireDelegationToken    | 1–2 | Wired | `DelegationTokenApiHandler` | |
| 41 | DescribeDelegationToken  | 1–3 | Wired | `DelegationTokenApiHandler` | |
| 50 | DescribeUserScramCredentials | 0 | Wired | `SecurityApiHandler` → `ScramCredentialStore.ListUsers` / `TryGetCredential` | KIP-554 SCRAM credential listing. Returns mechanism + iterations per user across both stores (SCRAM-SHA-256 + SHA-512). Empty / null user list enumerates everything. Users with no credential surface as `ResourceNotFound` with a documented reason. 6 tests. |
| 51 | AlterUserScramCredentials    | 0 | Wired | `SecurityApiHandler` → `ScramCredentialStore.AddCredential` / `RemoveUser` | KIP-554 SCRAM credential upsert + delete. Per-row error coding. Upsertion uses the client-supplied `SaltedPassword` / `Salt` / `Iterations` and derives `StoredKey` + `ServerKey` per RFC 5802 (SHA-256 or SHA-512 keyed off the mechanism). Validates non-empty username, positive iterations, non-empty salt + salted password. Stores are stood up in-memory when SCRAM is in `SaslMechanisms` — no static config file; the wire RPC is the canonical provisioning path. |

### Quotas

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 48 | DescribeClientQuotas | 0–1 | Wired | `QuotaApiHandler` | |
| 49 | AlterClientQuotas    | 0–1 | Wired | `QuotaApiHandler` | |

### Cluster, log dirs, producers

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 23 | OffsetForLeaderEpoch | — | Not implemented | — | |
| 34 | AlterReplicaLogDirs  | 1–2 | Wired (rejects) | `TopicAdminHandler` | KIP-113 partition log-dir move. Surgewave runs with a single data dir per broker — every requested partition is rejected with `LogDirNotFound` so admin tools see the precise reason. |
| 35 | DescribeLogDirs      | 1–5 | Wired | `TopicAdminHandler` | Single `LogDirResult` entry per response with `LogDir` = `LogManager.DataDirectory` and per-partition `TotalSize` from each `IPartitionLog.TotalSize`. v4+ `TotalBytes` / `UsableBytes` come from `DriveInfo`; -1 if the volume isn't readable. Topic filter narrows projection. 4 tests covering filter / no-filter / volume-bytes / reject. |
| 56 | AlterPartition       | — | Not implemented | — | Controller responsibility. |
| 57 | UpdateFeatures       | — | Not implemented | — | |
| 58 | Envelope             | — | Not implemented | — | KRaft request envelope. |
| 61 | DescribeProducers    | 0   | Wired | `SurgewaveBroker.ProcessRequestAsync` → `TransactionCoordinator.HandleDescribeProducers` | KIP-664. Per-partition listing of active producer state from `ProducerStateManager` — producers that never wrote to or held a transaction on a partition are excluded. `LastTimestamp` and `CoordinatorEpoch` are returned as -1 (Surgewave doesn't track them per partition); `CurrentTxnStartOffset` is 0 when the producer holds an open transaction on the partition, -1 otherwise. 5 tests. |
| 64 | UnregisterBroker     | 0   | Wired (controller) | `ClusterMembershipHandler` | KIP-895. |

### Inter-broker / KRaft

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 4  | LeaderAndIsr        | 0–7 | Wired | `InterBrokerApiHandler` | |
| 5  | StopReplica         | 0–3 | Wired | `InterBrokerApiHandler` | |
| 6  | UpdateMetadata      | 0–8 | Wired | `InterBrokerApiHandler` | |
| 7  | ControlledShutdown  | 0–3 | Wired | `InterBrokerApiHandler` | |
| 52 | Vote                | 0–1 | Wired | `RaftApiHandler` | |
| 53 | BeginQuorumEpoch    | 0–1 | Wired | `RaftApiHandler` | |
| 54 | EndQuorumEpoch      | 0–1 | Wired | `RaftApiHandler` | |
| 55 | DescribeQuorum      | 0–3 | Wired | `RaftApiHandler` | |
| 59 | FetchSnapshot       | 0   | Wired | `RaftApiHandler` | |
| 62 | BrokerRegistration  | 0–4 | Wired | `ClusterMembershipHandler` | |
| 63 | BrokerHeartbeat     | 0–4 | Wired | `ClusterMembershipHandler` | |
| 70 | ControllerRegistration | — | Not implemented | — | |
| 73 | AssignReplicasToDirs   | — | Not implemented | — | |
| 80 | AddRaftVoter        | 0   | Wired (rejects) | `RaftApiHandler` | KIP-853 online voter reconfiguration is **not implemented** in this Surgewave release; the handler returns `UnsupportedVersion (35)` with a stable message pointing operators at static config + restart. The wire is bound (rather than left to the dispatcher's generic no-handler fallback) so admin tools see a precise error code instead of "version mismatch" and do not retry across versions. |
| 81 | RemoveRaftVoter     | 0   | Wired (rejects) | `RaftApiHandler` | Same as above. |
| 82 | UpdateRaftVoter     | 0   | Wired (rejects) | `RaftApiHandler` | Same as above. |

### Telemetry (KIP-714)

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 71 | GetTelemetrySubscriptions | 0 | Wired | `TelemetryApiHandler` | When `Surgewave:Telemetry:Enabled=true`, returns the configured push interval and metric subscription set so librdkafka 2.x clients start pushing OTLP metrics. When disabled (default), advertises an empty set with a 5-min backoff — preserves the pre-G9 stub behaviour so an upgrade doesn't change wire traffic. |
| 72 | PushTelemetry             | 0 | Wired | `TelemetryApiHandler` → `ITelemetryIngestor` | Default `LoggingTelemetryIngestor` logs each push and emits `surgewave.broker.client_telemetry.{pushes_received, bytes_received, terminating_pushes}` counters on the broker meter. Operators plug in a custom `ITelemetryIngestor` to forward OTLP payloads to a collector or topic. Payload bytes themselves are not decoded — clients already produce OTLP-compatible streams that downstream tooling can consume directly. |

---

## KIP coverage

| KIP | Title | Status | Evidence |
|-----|-------|--------|----------|
| KIP-8   | Replica Fetcher                          | Done    | `InterBrokerApiHandler.UpdateMetadata` |
| KIP-98  | Exactly-Once Delivery (idempotent + txns) | Done | `TransactionCoordinator`, `Kip892TransactionDefenseTests`, six `TransactionTests` (commit, abort, isolation, mixed, fencing, consume-process-produce) round-trip via Confluent.Kafka. The five EOS RPCs (22, 24, 25, 26, 28) dispatch through the `SurgewaveBroker.ProcessRequestAsync` fast-path switch. |
| KIP-516 | Topic Identifiers                        | Done    | `Metadata`/`Fetch`/`Produce` carry `Uuid` topic IDs |
| KIP-595 | KRaft (ZK-less consensus)                | Done    | `RaftApiHandler`, 5 quorum APIs (52–55, 59) |
| KIP-714 | Client Telemetry Reporting               | Done (wire + log/meter ingestor + topic sink) | `TelemetryApiHandler` + `ITelemetryIngestor`; default `LoggingTelemetryIngestor` logs and meters every push. Opt-in via `Surgewave:Telemetry:Enabled=true`. Optional `TelemetryTopicSink` (`Surgewave:Telemetry:TopicSinkEnabled=true`) mirrors raw OTLP MetricsData blobs to an internal topic with `client.id` / `compression` / `terminating` / `subscription.id` headers — downstream OTLP collectors read them as a Kafka stream. 11 tests across `TelemetryApiHandlerTests` and `TelemetryTopicSinkTests`. Server-side decode of the OTLP proto into per-metric Meter instruments remains a future enhancement; topic-sinking the raw bytes covers the common operator path (collector picks up the topic and feeds Prometheus/Grafana/etc.). |
| KIP-936 | SASL/OAUTHBEARER on the wire             | Done    | `OAuthBearerAuthenticator` + `JwksTokenValidator`, wired into `SaslAuthenticator`; OIDC discovery + direct JWKS, audience/issuer/principal-claim configurable; `RequireHttpsMetadata` flag for in-process IdP fixtures; 12 unit tests (`OAuthBearerAuthenticatorTests`, `SaslOAuthBearerWiringTests`) plus 5 integration tests (`OAuthBearerSaslIntegrationTests`, `OAuthBearerValidatorProbeTests`) that round-trip a Confluent.Kafka producer/consumer through an in-process IdP |
| KIP-848 | Consumer Group v2                        | Done    | `ConsumerGroupV2*`, `Kip848WireTests`, `Kip848ConsumerProtocolTests` |
| KIP-853 | Dynamic Raft Voter Changes               | Wire bound + foundation in place — semantics not implemented | RPCs 80/81/82 advertise and bind to a polite-rejection handler. Pre-validation rejects malformed requests (negative voter id, empty listener list) with `InvalidRequest (42)` ahead of the not-supported reply. Foundation building blocks for a future implementation are committed: immutable `RaftConfiguration` record with `AddVoter` / `RemoveVoter` / `UpdateVoter` mutations + monotonic `ConfigurationSequence` (13 tests), reserved `MetadataCommandType.VoterChange` enum slot. Implementation plan documented in [`docs/raft/voter-changes.md`](docs/raft/voter-changes.md) — single-server-change per Ongaro §4.2, ~5 days estimated, requires linearizability validation against `Kuestenlogik.Surgewave.Testing.Chaos`. |
| KIP-892 | Transaction Coordinator Schemas          | Done    | `Kip892TransactionDefenseTests` |
| KIP-894 | Tiered Storage (Remote Log)              | Partial | `RemoteLogSegmentState`, `IRemoteStorageProvider` plug-points exist; remote fetch path is fenced behind config |
| KIP-895 | KRaft broker registration v2             | Done    | `ClusterMembershipHandler` (62, 63, 64) |
| KIP-903 | Fetch v15 (replicaId removed)            | Done    | `FetchRequest.Reader` flexible-version parsing |
| KIP-932 | Share Groups (queue-style)               | Done    | `ShareGroupApiHandler`, `ShareGroupCoordinator`, `Kip932WireTests` |
| KIP-985 | Reverse range iteration                  | Done    | `IReadOnlyKeyValueStore.ReverseAll()` / `.ReverseRange()` |
| KIP-994 | Transactional ListTransactions filters   | Done | `Kip994ListTransactionsFiltersTests` (8 semantic) + `Kip994ListTransactionsWireTests` (4 wire-roundtrip across v0/v1/v2). Pathological-regex defence (`(a+)+$` Cox bait) is caught by the per-Regex `MatchTimeout` plus a `RegexMatchTimeoutException` swallow in `ListTransactions` — without that swallow the timeout fires but the exception bubbled up and crashed the listing. |
| KIP-1071 | Streams Groups (topology-aware)         | Done    | `StreamsGroupApiHandler`, `StreamsGroupCoordinator` |

---

## Conformance test infrastructure

| Suite | Location | What it pins down |
|-------|----------|-------------------|
| Wire encoder / decoder | `tests/Kuestenlogik.Surgewave.Protocol.Kafka.Tests/Kip848WireTests.cs`, `Kip932WireTests.cs`, `KafkaProtocolHandlerTests.cs` | Frame parsing, flexible-version tagged fields, byte-level equivalence for KIP-848 / KIP-932 messages. |
| API coverage matrix | `tests/Kuestenlogik.Surgewave.IntegrationTests/KafkaProtocolApiCoverageTests.cs` | `Confluent.Kafka.Admin` exercises Metadata, Produce, Fetch, ApiVersions against an embedded broker; verifies advertised-versions match what the broker actually services. |
| Confluent.Kafka round-trip | `tests/Kuestenlogik.Surgewave.IntegrationTests/ConfluentKafkaCompatibilityTests.cs` | End-to-end producer + consumer using Confluent.Kafka 2.x against an embedded Surgewave broker — green. |
| KIP-848 client interop | `tests/Kuestenlogik.Surgewave.IntegrationTests/Kip848ConsumerProtocolTests.cs`, `Kip848DiagnosticTests.cs` | librdkafka 2.14 next-gen consumer flow; CGv2 group join, server-side assignment, rebalance — green. |
| KIP-892 transaction defense | `tests/Kuestenlogik.Surgewave.Broker.Tests/Transactions/Kip892TransactionDefenseTests.cs` | Producer epoch fencing, abort-on-timeout, two-phase commit. |
| KIP-994 list-transactions filters | `tests/Kuestenlogik.Surgewave.Broker.Tests/Transactions/Kip994ListTransactionsFiltersTests.cs` | `producerIdFilter` + `statesFilter` semantics. |
| KIP-936 OAUTHBEARER e2e | `tests/Kuestenlogik.Surgewave.IntegrationTests/OAuthBearerSaslIntegrationTests.cs`, `OAuthBearerValidatorProbeTests.cs`, `Fixtures/OAuthBearerBrokerFixture.cs` | In-process IdP serves OIDC discovery + JWKS over `HttpListener`; embedded broker validates a freshly-signed RS256 JWT presented through Confluent.Kafka's `OAuthBearerTokenRefreshHandler`. Asserts produce + consume round-trip and that a wrong-issuer token is rejected. |

### Cross-client matrix (status)

| Client | Producer | Consumer (classic) | Consumer (CGv2) | Transactions | Share Groups | Notes |
|--------|----------|--------------------|-----------------|--------------|--------------|-------|
| Confluent.Kafka 2.x (.NET) | Tested | Tested | Tested | Not tested e2e | Not tested e2e | Primary in-tree integration suite. |
| librdkafka 2.14 (C / native) | Tested | Tested | Tested | Not tested e2e | Not tested e2e | Driven through Confluent.Kafka. |
| Kafka Java client | Not tested | Not tested | Not tested | Not tested | Not tested | On the G2 follow-up backlog. |
| Sarama / kgo (Go) | Not tested | Not tested | Not tested | Not tested | Not tested | On the G2 follow-up backlog. |
| kafka-python / aiokafka | Not tested | Not tested | Not tested | Not tested | Not tested | On the G2 follow-up backlog. |

---

## Known gaps

1. **KIP-714 server-side OTLP decode.** The wire is bound, payloads are
   logged + metered, and the optional topic sink mirrors raw OTLP bytes
   to an internal topic. Decoding the proto into per-metric Meter
   instruments on the broker would surface client metrics in the broker's
   own dashboards without an external collector — this is a future
   enhancement; the topic-sink path already covers the common operator
   workflow (collector reads the topic, feeds Prometheus/Grafana).
2. **All admin RPCs are now wired.** Previous editions of this list called
   out `ElectLeaders`, `AlterPartitionReassignments`,
   `ListPartitionReassignments`, `DescribeLogDirs`, `AlterReplicaLogDirs`,
   `DescribeUserScramCredentials`, `AlterUserScramCredentials`,
   `ListConfigResources` as missing — those are now bound to either a
   real handler (most of the list) or a polite-reject handler
   (`AlterReplicaLogDirs` rejects every partition with `LogDirNotFound`
   because Surgewave has no JBOD; KIP-853 voter-change RPCs reject with
   `UnsupportedVersion` because Surgewave's Raft has no online
   reconfiguration yet — see [docs/raft/voter-changes.md](docs/raft/voter-changes.md)).
3. **KIP-853 online voter changes** are not implemented. RPCs 80/81/82 are
   wire-bound but reject with a documented `UnsupportedVersion`. Operators
   reconfigure the static voter set and restart. Implementing the
   joint-consensus / single-server-change protocol is tracked separately.
4. **Cross-client conformance matrix** currently covers only the
   librdkafka / Confluent.Kafka path. Java, Go, and Python client e2e suites
   are scheduled.

---

## Confluent Schema Registry compatibility

Surgewave ships its own Schema Registry under `src/Kuestenlogik.Surgewave.Schema.Registry`. To
be a drop-in replacement for Confluent Schema Registry, Surgewave must match two
verifiable contracts: the magic-byte wire format embedded in every record, and
the REST API surface that schema-aware clients call. Both have been audited
(G16 of the competitive gap analysis); this section is the per-contract
status statement.

### Magic-byte wire format

Confluent's wire layout in every record value:

```
0x00 (1 byte)        — magic byte
schemaId (4 bytes)   — schema id, big-endian int32
[messageIndex]       — Protobuf only: ZigZag-varint message-index array
payload              — bytes
```

| Aspect | Surgewave status | Source |
|--------|-------------|--------|
| Magic byte 0x00 | ✓ Wired | `src/Kuestenlogik.Surgewave.Client.SchemaRegistry/SchemaRegistrySerializerConfig.cs` (`WriteHeader` / `ReadSchemaId`) |
| SchemaId big-endian int32 | ✓ Wired | same |
| Avro / JSON payload follows immediately | ✓ Wired | `SchemaRegistryAvroSerializer`, `SchemaRegistryJsonSerializer` |
| Protobuf MessageIndex (single-message) | ✓ Wired (writes `0x00`, reads via `SkipVarint`) | `SchemaRegistryProtobufSerializer` / `Deserializer` |
| Protobuf MessageIndex (multi-message, KIP-style nested types) | ✗ Not implemented — index is hard-coded to 0 | gap |

### REST API path coverage

Surgewave hosts the API under `src/Kuestenlogik.Surgewave.Schema.Registry.Hosting/SchemaRegistryRestApi.cs`.

| Confluent path | Method | Surgewave status | Notes |
|----------------|--------|-------------|-------|
| `/subjects` | GET | ✓ | Returns `string[]` |
| `/subjects/{subject}/versions` | GET | ✓ | Returns `int[]` |
| `/subjects/{subject}/versions` | POST | ✓ | Body `RegisterSchemaRequest` → `{"id":int}` |
| `/subjects/{subject}/versions/{version}` | GET | ✓ | Full `SchemaResponse` |
| `/subjects/{subject}/versions/latest` | GET | ✓ | Same shape as above |
| `/subjects/{subject}` | POST | ✓ | Schema lookup by content |
| `/schemas/ids/{id}` | GET | ✓ | `GetSchemaByIdResponse` |
| `/schemas/ids/{id}/versions` | GET | ✓ | `IReadOnlyList<SubjectVersion>` |
| `/schemas/types` | GET | ✓ | Returns `string[]` of supported types |
| `/compatibility/subjects/{subject}/versions/{version}` | POST | ✓ | `CompatibilityCheckResponse` |
| `/config` | GET / PUT | ✓ | `ConfigResponse` |
| `/config/{subject}` | GET / PUT / DELETE | ✓ | per-subject override |
| `/mode` | GET / PUT | Partial | basic mode response, advanced read-only / read-write modes simplified |

Surgewave-only extensions (do not interfere with Confluent clients):
`/schemas/infer/{topic}`, `/schemas/infer/{topic}/register`,
`/api/schema-evolution/*`, `/api/schema-migration/*`.

### Compatibility levels

Surgewave accepts and emits all seven Confluent levels: `NONE`, `BACKWARD`,
`BACKWARD_TRANSITIVE`, `FORWARD`, `FORWARD_TRANSITIVE`, `FULL`,
`FULL_TRANSITIVE` (case-insensitive on input, uppercase on output).

### Schema types

`AVRO`, `JSON`, `PROTOBUF` are accepted on input and emitted on output,
case-insensitive on input. Surgewave additionally accepts `FLATBUFFERS` —
this is a Surgewave extension; standard Confluent clients ignore unknown types.

### JSON shape contract

Pinned by `tests/Kuestenlogik.Surgewave.Schema.Registry.Tests/ConfluentSchemaRegistryContractTests.cs`
(28 tests). The tests guard against silent regressions in the field-naming
convention — a stray `errorCode` (camelCase) instead of `error_code`
(snake_case) would break every Confluent client without showing up as a
type or unit-test failure elsewhere.

| Response | Shape |
|----------|-------|
| `ErrorResponse` | `{"error_code": int, "message": string}` (snake_case — this is intentional and matches Confluent's historical inconsistency) |
| `CompatibilityCheckResponse` | `{"is_compatible": bool, "messages": string[]?}` |
| `ConfigResponse` | `{"compatibilityLevel": string}` (camelCase here — Confluent is inconsistent across endpoints; we follow them exactly) |
| `SchemaResponse` | `{"subject", "id", "version", "schemaType", "schema", "references"}` |
| `RegisterSchemaResponse` | `{"id": int}` only |

### Known gaps

- **Multi-message Protobuf** — Surgewave always writes/reads MessageIndex 0.
  This is the common case; multi-message Protobuf schemas (`message A {} message B {}`
  in one .proto) need the full ZigZag-varint array. Tracked.
- **End-to-end against `Confluent.SchemaRegistry` .NET client** — the
  contract tests cover JSON shapes and the magic-byte wire format
  byte-by-byte. A live round-trip with `CachedSchemaRegistryClient`
  registering / fetching / serializing through Surgewave's REST API is the next
  layer of confidence.
- **Schema References** (Confluent KIP-718) — `references` field is
  serialized in responses, but the resolver path that walks references on
  fetch needs verification against multi-schema imports.

---

## Non-goals (intentional)

- **Kafka MirrorMaker 2 control plane.** Surgewave.Replication ships its own
  cluster-link control plane; MirrorMaker is not emulated.
- **ZooKeeper protocol.** Surgewave is KRaft-native; the legacy ZK control plane
  is intentionally absent.
- **Wire-level emulation of internal Kafka controller RPCs** (`AlterPartition`,
  `Envelope`, `ControllerRegistration`, `AssignReplicasToDirs`). Surgewave has its
  own controller; clients never see these.

---

## How this document is maintained

- The Kafka API matrix is generated from the same `ApiVersionsResponse.CreateDefault`
  source used at runtime and the per-handler `SupportedApiKeys` lists in
  `src/Kuestenlogik.Surgewave.Broker/Handlers/`. When you add a handler or advertise a new
  version, update both the source and this document in the same PR — CI checks
  for drift.
- Conformance test additions land alongside their RPCs and are listed in the
  test-infrastructure table above.
- KIPs are added to the coverage table when their first wire artefact lands,
  and the status column is updated as semantics catch up.

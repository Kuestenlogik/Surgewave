# Kafka API matrix

Per-RPC status across all Kafka 4.x API keys. Versions are the range Surgewave
advertises in `ApiVersions`. "Kafka" is the latest range published by
Apache Kafka 4.2 — they match unless noted.

See the [overview page](index.md) for headline numbers, status-legend
explanations, and source-of-truth pointers. The canonical document lives at
[`CONFORMANCE.md`](https://github.com/Kuestenlogik/Surgewave/blob/main/CONFORMANCE.md)
in the repository root.

## Core data plane

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 0  | Produce            | 0–13 | Wired | `DataApiHandler` | Quotas, dedup, deferred delivery, TTL. MinVersion 0 (not 3) is intentional for librdkafka compression detection. |
| 1  | Fetch              | 4–18 | Wired | `DataApiHandler` | Zero-copy, tiered storage hand-off, Topic IDs (KIP-516), incremental fetch sessions. |
| 2  | ListOffsets        | 1–11 | Wired | `DataApiHandler` | Timestamp + offset lookup, leader-epoch aware. |
| 3  | Metadata           | 0–13 | Wired | `MetadataApiHandler` | Auto-topic-create hook, broker discovery. |
| 18 | ApiVersions        | 0–5  | Wired | `MetadataApiHandler` | Advertises `group.version`, `transaction.version`, `metadata.version` features. |
| 60 | DescribeCluster    | 0–2  | Wired | `MetadataApiHandler` | KIP-700. |

## Consumer Group classic (v1)

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
| 47 | OffsetDelete     | 0    | Wired | `ConsumerGroupApiHandler` | KIP-496. Conservative: a group with active members rejects the delete with `GROUP_SUBSCRIBED_TO_TOPIC` for every requested partition (Surgewave doesn't decode the classic-protocol Subscription bytes). Empty groups always allow. |

## Consumer Group v2 (KIP-848)

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 68 | ConsumerGroupHeartbeat | 0–1 | Wired | `ConsumerGroupV2ApiHandler` | Server-side assignment, no SyncGroup round-trip. `ConsumerGroupV2Reconciler` + `ConsumerGroupV2Coordinator` ship full state machine; persistence verified by `ConsumerGroupV2PersistenceTests`. |
| 69 | ConsumerGroupDescribe  | 0–1 | Wired | `ConsumerGroupV2ApiHandler` | |

## Share Groups (KIP-932)

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

## Streams Groups (KIP-1071)

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 88 | StreamsGroupHeartbeat | 0 | Wired | `StreamsGroupApiHandler` | Topology-aware sticky assignment. |
| 89 | StreamsGroupDescribe  | 0 | Wired | `StreamsGroupApiHandler` | |

## Transactions (KIP-98 / KIP-892 / KIP-994)

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 22 | InitProducerId       | 0–6 | Wired | `SurgewaveBroker.ProcessRequestAsync` → `TransactionCoordinator.HandleInitProducerIdAsync` | Fast-path switch dispatches transaction RPCs ahead of the `RequestDispatcher`. KIP-892 epoch fencing enforced (`Kip892TransactionDefenseTests`). |
| 24 | AddPartitionsToTxn   | 0–5 | Wired | same → `HandleAddPartitionsToTxn` | |
| 25 | AddOffsetsToTxn      | 0–4 | Wired | same → `HandleAddOffsetsToTxn` | Exercised by `TransactionTests.ConsumeProcessProduce_AtomicallyCommitsOffsetsAndOutputs`. |
| 26 | EndTxn               | 0–5 | Wired | same → `HandleEndTxnAsync` | Writes commit/abort markers to all participating partitions. |
| 27 | WriteTxnMarkers      | 0–1 | Wired | `InterBrokerApiHandler` | Inter-broker only. |
| 28 | TxnOffsetCommit      | 0–5 | Wired | same → `HandleTxnOffsetCommit` | Exercised by `TransactionTests.ConsumeProcessProduce_AtomicallyCommitsOffsetsAndOutputs`. |
| 65 | DescribeTransactions | 0   | Wired | `SurgewaveBroker.ProcessRequestAsync` → `TransactionCoordinator.HandleDescribeTransactions` | Per-id projection onto on-the-wire `TransactionState` shape; unknown ids surface as error rows rather than being dropped. |
| 66 | ListTransactions     | 0–2 | Wired | same → `HandleListTransactions` | Threads `StateFilters` / `ProducerIdFilters` / KIP-994 `DurationFilter` / KIP-1152 `TransactionalIdPattern` straight through; pathological-regex DoS defence applies on the wire path too. |
| 67 | AllocateProducerIds  | 0–1 | Wired (controller-side) | — | Producer ID pool inside `ProducerStateManager`. |

> Confluent.Kafka 2.x transactional clients (`InitTransactions` / `BeginTransaction` /
> `ProduceAsync` / `SendOffsetsToTransaction` / `CommitTransaction`) round-trip green
> against an embedded Surgewave broker — see the six tests in
> `tests/Kuestenlogik.Surgewave.IntegrationTests/TransactionTests.cs` (commit, abort, isolation
> levels, mixed transactions, producer fencing, consume-process-produce EOS).

## Topic & partition admin

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 19 | CreateTopics       | 2–7 | Wired | `TopicAdminHandler` | |
| 20 | DeleteTopics       | 1–6 | Wired | `TopicAdminHandler` | |
| 21 | DeleteRecords      | 0–2 | Wired | `TopicAdminHandler` | Truncate at offset. |
| 37 | CreatePartitions   | 0–3 | Wired | `TopicAdminHandler` | |
| 75 | DescribeTopicPartitions | 0 | Wired | `MetadataApiHandler` | KIP-966 paginated metadata fetch — used by Java Kafka client 4.0+ to walk large clusters in bounded chunks. Honours `ResponsePartitionLimit` and `StartingCursor`; emits `NextCursor` when truncated. |
| 43 | ElectLeaders            | 0–2 | Wired | `ClusterAdminHandler` → `ClusterController.ElectLeaderAsync` | KIP-460. Per-partition: prefers an ISR member, falls back to first ISR, then unclean (any alive replica) when ISR is empty. Top-level error = `NotController` when this broker isn't the cluster controller. |
| 45 | AlterPartitionReassignments | 0–1 | Wired | `ClusterAdminHandler` → `PartitionReassignmentManager.ExecuteReassignmentAsync` | KIP-455. Builds a `ReassignmentPlan` and hands it to the manager's 4-state machine (Pending → Adding → Syncing → Completing → Completed). Cancel-via-empty-Replicas surfaces as `InvalidRequest` per partition — the manager doesn't expose a cancel API yet. |
| 46 | ListPartitionReassignments  | 0   | Wired | `ClusterAdminHandler` → `PartitionReassignmentManager.GetActiveReassignments` | Filters by topic if specified; returns active state with `TargetReplicas` / `AddingReplicas` / `RemovingReplicas`. |

## Configuration

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 32 | DescribeConfigs            | 1–4 | Wired | `ConfigApiHandler` | |
| 33 | AlterConfigs               | 0–2 | Wired | `ConfigApiHandler` | |
| 44 | IncrementalAlterConfigs    | 0–1 | Wired | `ConfigApiHandler` | |
| 74 | ListConfigResources        | 0–1 | Wired | `TelemetryApiHandler` | KIP-1106. Returns the configured `ClientTelemetryConfig.RequestedMetrics` as `ConfigResource` rows when telemetry is enabled; empty list otherwise. |

## Security

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
| 50 | DescribeUserScramCredentials | 0 | Wired | `SecurityApiHandler` → `ScramCredentialStore.ListUsers` | KIP-554 SCRAM credential listing. Returns mechanism + iterations per user across both stores (SHA-256 + SHA-512). Empty user list enumerates everything. Users with no credential surface as `ResourceNotFound`. |
| 51 | AlterUserScramCredentials    | 0 | Wired | `SecurityApiHandler` → `ScramCredentialStore.AddCredential` / `RemoveUser` | KIP-554 SCRAM upsert + delete. Per-row error coding. Upsertion derives `StoredKey` + `ServerKey` per RFC 5802. Stores stand up in-memory when SCRAM is in `SaslMechanisms` — the wire RPC is the canonical provisioning path. |

## Quotas

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 48 | DescribeClientQuotas | 0–1 | Wired | `QuotaApiHandler` | |
| 49 | AlterClientQuotas    | 0–1 | Wired | `QuotaApiHandler` | |

## Cluster, log dirs, producers

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 23 | OffsetForLeaderEpoch | — | Not implemented | — | |
| 34 | AlterReplicaLogDirs  | 1–2 | Wired (rejects) | `TopicAdminHandler` | KIP-113 partition log-dir move. Surgewave runs with a single data dir per broker — every requested partition is rejected with `LogDirNotFound` so admin tools see the precise reason. |
| 35 | DescribeLogDirs      | 1–5 | Wired | `TopicAdminHandler` | Single `LogDirResult` entry per response with `LogDir` = `LogManager.DataDirectory` and per-partition `TotalSize` from each `IPartitionLog.TotalSize`. v4+ `TotalBytes` / `UsableBytes` come from `DriveInfo`; -1 if the volume isn't readable. |
| 56 | AlterPartition       | — | Not implemented | — | Controller responsibility. |
| 57 | UpdateFeatures       | — | Not implemented | — | |
| 58 | Envelope             | — | Not implemented | — | KRaft request envelope. |
| 61 | DescribeProducers    | 0   | Wired | `SurgewaveBroker.ProcessRequestAsync` → `TransactionCoordinator.HandleDescribeProducers` | KIP-664. Per-partition listing of active producer state — producers that never wrote to or held a transaction on a partition are excluded. `LastTimestamp` and `CoordinatorEpoch` are returned as -1 (Surgewave doesn't track them per partition); `CurrentTxnStartOffset` is 0 when the producer holds an open transaction on the partition, -1 otherwise. |
| 64 | UnregisterBroker     | 0   | Wired (controller) | `ClusterMembershipHandler` | KIP-895. |

## Inter-broker / KRaft

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
| 80 | AddRaftVoter        | 0   | Wired (rejects) | `RaftApiHandler` | KIP-853 online voter reconfiguration is **not implemented** in this Surgewave release; the handler returns `UnsupportedVersion (35)` with a stable message pointing operators at static config + restart. The wire is bound (rather than left to the dispatcher's generic no-handler fallback) so admin tools see a precise error code instead of "version mismatch". |
| 81 | RemoveRaftVoter     | 0   | Wired (rejects) | `RaftApiHandler` | Same as above. |
| 82 | UpdateRaftVoter     | 0   | Wired (rejects) | `RaftApiHandler` | Same as above. |

## Telemetry (KIP-714)

| Key | API | Surgewave | Status | Handler | Notes |
|---:|-----|-------|--------|---------|-------|
| 71 | GetTelemetrySubscriptions | 0 | Wired | `TelemetryApiHandler` | When `Surgewave:Telemetry:Enabled=true`, returns the configured push interval and metric subscription set so librdkafka 2.x clients start pushing OTLP metrics. When disabled (default), advertises an empty set with a 5-min backoff — preserves the pre-G9 stub behaviour so an upgrade doesn't change wire traffic. |
| 72 | PushTelemetry             | 0 | Wired | `TelemetryApiHandler` → `ITelemetryIngestor` | Default `LoggingTelemetryIngestor` logs each push and emits `surgewave.broker.client_telemetry.{pushes_received, bytes_received, terminating_pushes}` counters on the broker meter. Operators plug in a custom `ITelemetryIngestor` to forward OTLP payloads to a collector or topic. |

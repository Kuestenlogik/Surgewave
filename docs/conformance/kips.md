# KIP coverage and conformance tests

Per-KIP status, conformance test inventory, cross-client matrix, and known
gaps. The [Kafka API matrix](kafka-rpcs.md) breaks the coverage down per
RPC; this page is the higher-level KIP view.

## KIP coverage

| KIP | Title | Status | Evidence |
|-----|-------|--------|----------|
| KIP-8   | Replica Fetcher                          | Done    | `InterBrokerApiHandler.UpdateMetadata` |
| KIP-98  | Exactly-Once Delivery (idempotent + txns) | Done | `TransactionCoordinator`, `Kip892TransactionDefenseTests`, six `TransactionTests` (commit, abort, isolation, mixed, fencing, consume-process-produce) round-trip via Confluent.Kafka. The five EOS RPCs (22, 24, 25, 26, 28) dispatch through the `SurgewaveBroker.ProcessRequestAsync` fast-path switch. |
| KIP-113 | Replicas to log dirs                     | Wired (rejects) | Surgewave has a single data dir per broker, so `AlterReplicaLogDirs` rejects every partition with `LogDirNotFound`. `DescribeLogDirs` returns the single LogDirResult with per-partition sizes. |
| KIP-455 | Partition reassignment                   | Done | `PartitionReassignmentManager` with full 4-state machine (Pending → Adding → Syncing → Completing → Completed); wired through `ClusterAdminHandler` for AlterPartitionReassignments / ListPartitionReassignments. |
| KIP-460 | Admin leader-election RPC                | Done | `ClusterAdminHandler` → `ClusterController.ElectLeaderAsync` with three-stage fallback (Preferred → ISR → Unclean). |
| KIP-496 | Allow consumers to delete offsets        | Done | `ConsumerGroupCoordinator.HandleOffsetDelete` — conservative read of GROUP_SUBSCRIBED_TO_TOPIC for groups with active members. |
| KIP-516 | Topic Identifiers                        | Done    | `Metadata`/`Fetch`/`Produce` carry `Uuid` topic IDs |
| KIP-554 | SCRAM credential admin RPCs              | Done | `SecurityApiHandler` + `ScramCredentialStore`. Two stores (SHA-256 + SHA-512) stand up in-memory when SCRAM is in `SaslMechanisms`. The wire RPC is the canonical provisioning path. |
| KIP-595 | KRaft (ZK-less consensus)                | Done    | `RaftApiHandler`, 5 quorum APIs (52–55, 59) |
| KIP-664 | DescribeProducers                        | Done | `TransactionCoordinator.HandleDescribeProducers` projects `ProducerStateManager` state per (topic, partition). |
| KIP-714 | Client Telemetry Reporting               | Done (wire + log/meter ingestor + topic sink) | `TelemetryApiHandler` + `ITelemetryIngestor`. Default `LoggingTelemetryIngestor` logs and meters every push. Optional `TelemetryTopicSink` mirrors raw OTLP MetricsData blobs to an internal topic. Server-side decode of the OTLP proto into per-metric Meter instruments remains a future enhancement. |
| KIP-848 | Consumer Group v2                        | Done    | `ConsumerGroupV2*`, `Kip848WireTests`, `Kip848ConsumerProtocolTests` |
| KIP-853 | Dynamic Raft Voter Changes               | Wire bound + foundation in place — semantics not implemented | RPCs 80/81/82 advertise and bind to a polite-rejection handler. Pre-validation rejects malformed requests with `InvalidRequest (42)`. Foundation building blocks: `RaftConfiguration` immutable record + monotonic `ConfigurationSequence`, reserved `MetadataCommandType.VoterChange` enum slot. Implementation plan: internal voter-changes design doc. |
| KIP-892 | Transaction Coordinator Schemas          | Done    | `Kip892TransactionDefenseTests` |
| KIP-894 | Tiered Storage (Remote Log)              | Partial | `RemoteLogSegmentState`, `IRemoteStorageProvider` plug-points exist; remote fetch path is fenced behind config |
| KIP-895 | KRaft broker registration v2             | Done    | `ClusterMembershipHandler` (62, 63, 64) |
| KIP-903 | Fetch v15 (replicaId removed)            | Done    | `FetchRequest.Reader` flexible-version parsing |
| KIP-932 | Share Groups (queue-style)               | Done    | `ShareGroupApiHandler`, `ShareGroupCoordinator`, `Kip932WireTests` |
| KIP-936 | SASL/OAUTHBEARER on the wire             | Done    | `OAuthBearerAuthenticator` + `JwksTokenValidator`, wired into `SaslAuthenticator`; OIDC discovery + direct JWKS, audience/issuer/principal-claim configurable; 12 unit tests + 5 integration tests with an in-process IdP fixture. |
| KIP-966 | Paginated DescribeTopicPartitions        | Done | `MetadataApiHandler.HandleDescribeTopicPartitions` honours `ResponsePartitionLimit` + `StartingCursor`; emits `NextCursor` when truncated. |
| KIP-985 | Reverse range iteration                  | Done    | `IReadOnlyKeyValueStore.ReverseAll()` / `.ReverseRange()` |
| KIP-994 | Transactional ListTransactions filters   | Done | `Kip994ListTransactionsFiltersTests` (8 semantic) + `Kip994ListTransactionsWireTests` (4 wire-roundtrip across v0/v1/v2). Pathological-regex defence (`(a+)+$` Cox bait) caught by `MatchTimeout` plus a `RegexMatchTimeoutException` swallow. |
| KIP-1071 | Streams Groups (topology-aware)         | Done    | `StreamsGroupApiHandler`, `StreamsGroupCoordinator` |
| KIP-1106 | ListConfigResources for KIP-714          | Done | `TelemetryApiHandler.HandleListConfigResources` returns the configured metric subscriptions when telemetry is enabled. |
| KIP-1152 | TransactionalIdPattern filter            | Done | Wire-parsed at v2+; the regex DoS defence (timeout + exception swallow) fires on this code path too. |
| KIP-1242 | ApiVersions v5 (ClusterId/NodeId + REBOOTSTRAP_REQUIRED) | Wire parsed; mismatch-detection deferred | `ApiVersionsRequest.cs` reads + writes the v5 `ClusterId` (nullable compact string) + `NodeId` (int32) fields so the trailing tagged-fields varint stays aligned. `ErrorCode.RebootstrapRequired` (129) is in the enum and ready to be returned by the handler. Actually triggering REBOOTSTRAP_REQUIRED on a ClusterId/NodeId mismatch is a follow-up — Surgewave today services any client that connects, and the IP-reuse-after-rebirth path (the KIP's motivating case) only matters once cluster-cross-replay is on the operator's radar. `Kip1242ApiVersionsV5Tests` pins the wire round-trip + the error-code value. |
| KIP-1226 | Share-partition lag in DescribeShareGroupOffsets v1 | Done | `DescribeShareGroupOffsetsResponse.DescribePartitionResult.Lag` (int64, default -1) on the wire, v1-gated in both WriteTo and ReadFrom. `ShareGroupCoordinator.HandleDescribeShareGroupOffsets` populates it via `log.HighWatermark - group.StartOffsets[key]` (floor 0) in both code paths — explicit topic list AND "all subscribed topics" fallback. `Kip1226ShareGroupLagTests` pins v0 drops the field (no framing drift) and v1 round-trips the lag value. |
| KIP-1222 | Renew acknowledgements in ShareAcknowledge v2 | Wire pinned; lease-renew semantics deferred | `ShareAcknowledgeRequest` carries the v2 `IsRenewAck` bool plus the new `AcknowledgeType = 4 (Renew)` value in the int8 array. `ShareAcknowledgeResponse` carries `AcquisitionLockTimeoutMs` (int32, v2+, ignorable). Both are v2-gated in WriteTo and ReadFrom. The actual lease-renew handling inside `ShareGroupCoordinator` is a follow-up — Surgewave currently treats a Renew ack as a no-op pass-through, which is wire-safe but doesn't actually extend the in-flight lock. `Kip1222ShareAckRenewTests` pins the v2 round-trip (IsRenewAck + AcknowledgeType=4) and a v1 regression that the v2 fields stay off the wire. |
| KIP-1319 | Topic IDs in TxnOffsetCommit v6 | Done | `TxnOffsetCommitRequest`/`Response` model refactored from name-keyed `Dictionary` to `List<TxnOffsetCommitTopic>` with both `Name?` (v0-5) and `TopicId` (v6+). WriteTo/ReadFrom pick the right wire field per version. Both `TransactionCoordinator` and `ClusteredTransactionCoordinator` resolve `TopicId → Name` via `LogManager.GetTopicMetadataById` at v6 entry and return `UNKNOWN_TOPIC_ID` per partition when unresolvable; pre-v6 paths back-fill `TopicId` so the response can carry it at v6 if needed. Advertised version bumped to 6. `Kip1319TxnOffsetCommitV6Tests` pins v5 (Name) and v6 (TopicId) wire framing for both request and response. |
| KIP-1251 | Per-partition assignment epochs (CGv2 internal state) | Structural state pinned; per-partition fence-check deferred | `TopicPartitionAssignment` carries a new nullable `AssignmentEpochs: List<int>?` aligned with `Partitions`. `TargetAssignmentComputer.ApplyAssignment` snapshots the previous (TopicId, Partition) → epoch map before recomputing so partitions that stay with the same member across a rebalance keep their stable per-partition epoch (the KIP's whole point — old commits for unchanged partitions stay valid); newly assigned partitions get the current `GroupEpoch`. JSON persistence is automatic via `[JsonInclude]`. The motivating use case — per-partition fence-check on `OffsetCommit` / `TxnOffsetCommit` — is a documented follow-up: Surgewave today fences group-level `MemberEpoch` which is strictly more conservative (more spurious fences than upstream, not fewer), so the state is captured and persisted now and the finer-grained fence can be wired in a separate, narrowly-scoped change without touching the live coordinator state-machine. `Kip1251PerPartitionEpochTests` pins: first compute populates with `GroupEpoch`, recompute with same membership keeps epochs stable, new-member-join makes reassigned partitions carry the new epoch while unchanged ones keep theirs, empty group is a no-op. All 17 pre-existing `ConsumerGroupV2*Tests` continue to pass. |

## Conformance test infrastructure

| Suite | Location | What it pins down |
|-------|----------|-------------------|
| Wire encoder / decoder | `tests/Kuestenlogik.Surgewave.Protocol.Kafka.Tests/Kip848WireTests.cs`, `Kip932WireTests.cs`, `Kip994ListTransactionsWireTests.cs`, `KafkaProtocolHandlerTests.cs` | Frame parsing, flexible-version tagged fields, byte-level equivalence for KIP-848 / KIP-932 / KIP-994 messages. |
| API coverage matrix | `tests/Kuestenlogik.Surgewave.IntegrationTests/KafkaProtocolApiCoverageTests.cs` | `Confluent.Kafka.Admin` exercises Metadata, Produce, Fetch, ApiVersions against an embedded broker; verifies advertised-versions match what the broker actually services. |
| Confluent.Kafka round-trip | `tests/Kuestenlogik.Surgewave.IntegrationTests/ConfluentKafkaCompatibilityTests.cs` | End-to-end producer + consumer using Confluent.Kafka 2.x against an embedded Surgewave broker. |
| KIP-848 client interop | `tests/Kuestenlogik.Surgewave.IntegrationTests/Kip848ConsumerProtocolTests.cs`, `Kip848DiagnosticTests.cs` | librdkafka 2.14 next-gen consumer flow; CGv2 group join, server-side assignment, rebalance. |
| KIP-892 transaction defense | `tests/Kuestenlogik.Surgewave.Broker.Tests/Transactions/Kip892TransactionDefenseTests.cs` | Producer epoch fencing, abort-on-timeout, two-phase commit. |
| KIP-994 list-transactions filters | `tests/Kuestenlogik.Surgewave.Broker.Tests/Transactions/Kip994ListTransactionsFiltersTests.cs` | `producerIdFilter` + `statesFilter` + `DurationFilter` + `TransactionalIdPattern` semantics, pathological-regex DoS defence. |
| KIP-936 OAUTHBEARER e2e | `tests/Kuestenlogik.Surgewave.IntegrationTests/OAuthBearerSaslIntegrationTests.cs`, `OAuthBearerValidatorProbeTests.cs`, `Fixtures/OAuthBearerBrokerFixture.cs` | In-process IdP serves OIDC discovery + JWKS over `HttpListener`; embedded broker validates a freshly-signed RS256 JWT presented through Confluent.Kafka's `OAuthBearerTokenRefreshHandler`. Asserts produce + consume round-trip and that a wrong-issuer token is rejected. |
| Schema Registry contract | `tests/Kuestenlogik.Surgewave.Schema.Registry.Tests/ConfluentSchemaRegistryContractTests.cs` | 28 tests pin the JSON-shape contract — error_code (snake_case), is_compatible (snake_case), compatibilityLevel (camelCase), schema-type uppercase mapping. |

## Cross-client matrix

| Client | Producer | Consumer (classic) | Consumer (CGv2) | Transactions | Share Groups | Notes |
|--------|----------|--------------------|-----------------|--------------|--------------|-------|
| Confluent.Kafka 2.x (.NET) | Tested | Tested | Tested | Tested | Not tested e2e | Primary in-tree integration suite. |
| librdkafka 2.14 (C / native) | Tested | Tested | Tested | Tested | Not tested e2e | Driven through Confluent.Kafka. |
| Kafka Java client | Not tested | Not tested | Not tested | Not tested | Not tested | On the follow-up backlog. |
| Sarama / kgo (Go) | Not tested | Not tested | Not tested | Not tested | Not tested | On the follow-up backlog. |
| kafka-python / aiokafka | Not tested | Not tested | Not tested | Not tested | Not tested | On the follow-up backlog. |

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
   reconfiguration yet).
3. **KIP-853 online voter changes** are not implemented. RPCs 80/81/82 are
   wire-bound but reject with a documented `UnsupportedVersion`. Operators
   reconfigure the static voter set and restart. Implementing the
   joint-consensus / single-server-change protocol is tracked separately —
   see the internal voter-changes design doc.
4. **Cross-client conformance matrix** currently covers only the
   librdkafka / Confluent.Kafka path. Java, Go, and Python client e2e
   suites are scheduled.

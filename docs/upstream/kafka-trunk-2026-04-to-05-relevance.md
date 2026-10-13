# Kafka trunk 5d03ccff57..HEAD (2026-04-04 .. 2026-05-11) — Surgewave relevance

193 commits reviewed. 26 classified as relevant to Surgewave. Assessed **only** from
the point of view of Kafka wire protocol compatibility and broker coordinator semantics —
Connect/Streams/Tools/Test/Build changes are excluded (see
Out-of-Scope below).

## Commits relevant to Surgewave

Sorted: high → medium → low.

| Commit | KAFKA ticket / KIP | Category | Surgewave relevance | What to follow up |
|---|---|---|---|---|
| 7562044781 | KAFKA-20444 [1/N] / KIP-1319 | wire-protocol | **high** — the new TxnOffsetCommit v6 adds `TopicId`, binds `Name` to v0-5; renames `GenerationId` -> `GenerationIdOrMemberEpoch` (semantically). The coordinator now waits for the MemberEpoch under the new consumer group protocol. | Add TxnOffsetCommitRequest/Response v6 to the Surgewave wire schema. Keep v6 unstable until the follow-up patches are through. |
| 2342c80dca | KAFKA-20444 [3/N] / KIP-1319 | wire-protocol | **high** — the builder validates: v6+ requires TopicId, v0-5 TopicName. | Analogous builder logic in Surgewave's producer code, if our own native client produces TxnOffsetCommit. |
| baa064e422 | KAFKA-20444 [4/N] / KIP-1319 | wire-protocol | **high** — TxnOffsetCommitResponse for TopicIds; `getErrorResponse` carries the TopicId through. | Follow up the response builder in the Surgewave coordinator, if TxnOffsetCommit is already implemented. |
| 319dd61cb3 | KAFKA-20444 [5/N] / KIP-1319 | wire-protocol+coordinator | **high** — for v6+ KafkaApis resolves the TopicName from the MetadataCache via TopicId; v0-5 backwards: TopicId via name. On failure `UNKNOWN_TOPIC_ID`. | Extend the handler in Surgewave's TxnOffsetCommit path: bidirectional resolution name<->ID + error codes. |
| 7f5861817d | KAFKA-20444 [6/N] / KIP-1319 | wire-protocol+client | **high** — the TransactionManager client now treats `GROUP_ID_NOT_FOUND` and `STALE_MEMBER_EPOCH` in addition to `ILLEGAL_GENERATION`/`UNKNOWN_MEMBER_ID` as a group metadata mismatch (CommitFailedException). | If Surgewave has its own native producer client with tx support: adapt the error handling for v6. Server side, make sure the coordinator returns these codes. |
| 6be48c6e54 | KAFKA-20441 | wire-protocol+kraft | **high** — removes `CordonedLogDirs` from BrokerRegistrationRequest v5; instead brings it as a nullable tagged field in BrokerHeartbeatRequest v2 (tag 1). `validVersions` falls back from 0-5 to 0-4. **Breaking** compared with an implementation that already spoke v5. | Remove BrokerRegistrationRequest/Response v5 from Surgewave again; treat BrokerHeartbeatRequest v2 `CordonedLogDirs` as nullable (null before the broker has reached the RECOVERY state). |
| 52687216c9 | KAFKA-20442 | coordinator | **high** — consumer group bug: when all members are fenced, `groupEpoch` is bumped but `assignmentEpoch` is not — violates the invariant `empty group => groupEpoch == assignmentEpoch`. The fix writes `ConsumerGroupTargetAssignmentMetadataRecord` with timestamp 0. | Check Surgewave's GroupCoordinator: implement the same path on member fence, follow up the target assignment epoch. |
| 56be35588a | KAFKA-20442 | coordinator | **high** — same problem for ShareGroups (`ShareGroupTargetAssignmentMetadataRecord`). | Analogously in Surgewave's ShareGroup coordinator. |
| e08a15091d | KAFKA-20442 | coordinator | **high** — same problem for StreamsGroups (`StreamsGroupTargetAssignmentMetadataRecord`). | Analogously in Surgewave's StreamsGroup coordinator (if implemented). |
| 0d9fe518b6 | KAFKA-20434 | coordinator | **high** — the consumer group does not recompute the assignment when all static members rejoin with a different server-side assignor. `bumpGroupEpoch` ignores a change of the preferred assignor. | Check Surgewave's group epoch bump: it must detect a change of the effective preferred server assignor (with default fallback). |
| d5d9868568 | KAFKA-20431 | wire-protocol+coordinator | **high** — ConsumerGroupDescribe response: `Assignment.assignedPartitions` did not contain `partitionsPendingRevocation` during reconciliation — partitions visibly "disappear" for clients. The fix merges both. | Check Surgewave's ConsumerGroupDescribe handler: `Assignment` must report `assignedPartitions ∪ partitionsPendingRevocation`. |
| 06f699664b | KAFKA-20415 / KIP-1191 | wire-protocol+coordinator | **medium** — new feature version `share.version=2` (gated DLQ support), new MetadataVersion `IBP_4_4_IV0`. ApiVersions now advertises SV_2 max. | Update Surgewave's ApiVersionsResponse feature advertising (ShareVersion 0/1/2). The DLQ server logic itself is KIP-1191-specific and can stay at SV_1 for now. |
| b6d2503710 | KAFKA-20410 / KIP-1191 | coordinator+config | **medium** — new group configs for the share group DLQ: `errors.deadletterqueue.auto.create.topics.enable`, `errors.deadletterqueue.replication.factor`, `errors.deadletterqueue.max.delivery.attempts`. | Extend Surgewave's GroupCoordinatorConfig + ShareGroupConfigs (defensively: accept + ignore if DLQ is not implemented). |
| cb7e3ab375 | KAFKA-20410 [2/N] / KIP-1191 | config | **medium** — new TopicConfig: `ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG` registered in LogConfig. | Extend LogConfig in Surgewave's storage engine analogously (tolerant default). |
| faa8b4870f | KAFKA-20533 | coordinator+wire | **medium** — ShareFetch: on topic deletion the error code in the response was wrong (`UNKNOWN_SERVER_ERROR` instead of the correct per-partition code because of an NPE with topic name = null). | Check Surgewave's ShareFetch handler: on topic deletion the correct per-partition error code must come back (no NPE, no generic error). |
| 44bafc60e7 | KAFKA-20426 | client-bug | **medium** — AsyncKafkaConsumer busy loop when `group.id` + `assign()` are combined: `maximumTimeToWait`=0 when the heartbeat is skipped. Not directly visible on the wire, but a client DDoS vector. | Surgewave's native .NET consumer (if present): check that no busy loop arises with manual assignment + group.id. |
| 4b9eddc132 | KAFKA-20428 | client-bug | **medium** — unsubscribe accidentally applied pending assignment updates from the background queue. | Check the native client logic, if implemented. |
| b954b35d0a | KAFKA-20332 | client-bug | **medium** — race: the app thread could deliver records for revoked partitions because revoke was not synchronised. Wire-visible effect: the consumer commits an offset for a foreign partition. | Check the native client / consumer reconciliation logic. |
| 7e1c9db92f | KAFKA-20332 [2] | client-bug | low — wakeup handling in the poll reconciliation check. | Follow-up patch to the one above, only for our own AsyncConsumer implementation. |
| 0749b320ba (see note) | — | (Streams-only) | low | (not relevant) |
| d0e0ec478c | KAFKA-20312 | client-bug | low — NPE in `OffsetFetcherUtils.regroupPartitionMapByNode` with a null leader during a metadata race. | Adopt the defensive null check, if we have our own offset fetcher. |
| b033b2e8f4 | KAFKA-20114 | client-bug | low — race in `RPCProducerIDManager` between `maybeRequestNextBlock()` and `handleUnsuccessfulResponse()` — premature retry. | Check defensively, if Surgewave has its own ProducerID manager client. |
| 123ee9e45d | KAFKA-20393 | client-bug | low — `TelemetrySender.stickyNode` used a stale IP after a broker address change (K8s pod replacement). | Check the telemetry client, if implemented. |
| 86328c25ee | KAFKA-19566 / KIP-1162 | quota | low — `ClientQuotaCallback#updateClusterMetadata` deprecated. It is removed in 5.0. Runtime behaviour unchanged. | Make a note, if Surgewave exposes a quota callback. |
| ff2ba93a5c | KAFKA-13022 | quota | low — `ClientQuotasImage#describe` perf optimisation via an index map. A pure server-side optimisation. | Possibly optimise Surgewave's quota image analogously, if it is a perf hotspot. |
| 22c1e445f1 | — | coordinator+config | low — better error messages with numeric bounds in GroupConfig validation. | Improve Surgewave's GroupConfig validator analogously (UX). |
| e4cd243abc | KAFKA-20337 | coordinator | low — refactoring: all `GroupConfig` fields become `Optional<T>`, broker defaults are resolved lazily — fix against stale capture on dynamic broker config changes. | Check Surgewave's GroupConfig for a similar stale capture issue. |
| 72e380c149 | KAFKA-20380 | kraft+config | low — the controller derives `advertised.listeners` from `controller.quorum.voters` when not defined. Wire effect only for the KRaft bootstrap. | Check Surgewave's controller listener resolution logic (probably implemented differently). |

## Recommended follow-up tickets for Surgewave

1. **Follow up KIP-1319 / TxnOffsetCommit v6** — top priority, since 6 patches were
   merged in a row. Wire schema (`TxnOffsetCommitRequest.json`,
   `TxnOffsetCommitResponse.json`) + server handler (name<->TopicId resolution
   incl. the `UNKNOWN_TOPIC_ID` error) + client error mapping for
   `GROUP_ID_NOT_FOUND` / `STALE_MEMBER_EPOCH`. Kafka refs:
   `7562044781`, `2342c80dca`, `baa064e422`, `319dd61cb3`, `7f5861817d`.
2. **GroupCoordinator: follow up the assignment epoch when all members are fenced** —
   in consumer group, ShareGroup, StreamsGroup. Check Surgewave's own
   implementation, fix the same bug if necessary. Kafka refs:
   `52687216c9`, `56be35588a`, `e08a15091d`.
3. **ConsumerGroupDescribe response: include the pending-revocation partitions** —
   otherwise partitions "disappear" during reconciliation from the point of view of
   admin tools. Kafka ref: `d5d9868568`.
4. **BrokerHeartbeat v2: `CordonedLogDirs` as a nullable tagged field; do NOT implement
   BrokerRegistration v5** (it was withdrawn). Surgewave should not introduce the
   v5 field, only the HeartbeatRequest variant. Kafka ref: `6be48c6e54`.
5. **Introduce the `share.version=2` feature level** — even without a full DLQ implementation
   ApiVersionsResponse must report SV_2 as max (as soon as DLQ server logic is planned).
   Until then deliberately leave it at SV_1. Kafka ref: `06f699664b`.
6. **ShareFetch error mapping on topic deletion** — no NPE, the correct
   per-partition error code instead of `UNKNOWN_SERVER_ERROR`. Kafka ref: `faa8b4870f`.
7. **Consumer group `bumpGroupEpoch` must detect a change of the preferred server
   assignor** — edge case on a static member rejoin with a different assignor.
   Kafka ref: `0d9fe518b6`.

## Implementation status (sprint 2026-05-11)

| Item | Status | Commit/Doc |
|---|---|---|
| KIP-1319 / TxnOffsetCommit v6 | **deferred** — Kafka itself has `latestVersionUnstable: true` | `concept/kip-1319-txn-offset-commit-v6.md` |
| KAFKA-20441 / BrokerHeartbeat v2 | **done** — tagged-field refactor + nullable CordonedLogDirs | `8f25f57` |
| KAFKA-20442 / Assignment epoch | **done** — verified, Surgewave's model is trivially the same | `432a918` |
| KAFKA-20434 / bumpGroupEpoch assignor | **done** — verified, stricter than Java by design | `b62e549` |
| KAFKA-20431 / ConsumerGroupDescribe pending-rev | **done** — union of assignment + owned | `dfe03e0` |
| KAFKA-20415 / share.version=1 | **done** — ApiVersionsResponse extended; SV_2 waits for DLQ | `d5c7cd1` |
| KAFKA-20410 / DLQ group configs | **noop** — Surgewave has no group resource config support; take it along when that is implemented | (no commit) |

## Out-of-Scope (deliberately not followed up)

Connect/MirrorMaker-internal changes (`dc5f816403`, `eef6cab648`,
`9c106dc9d0`), Streams DSL/state store changes (`94b6886b12`, `0749b320ba`,
KAFKA-20194/20329/20173/20307/20422 header stores, KAFKA-20396/20398/20456/20264/20194 etc.),
ConfigCommand/Tools/Docs/Test refactorings (KAFKA-20297 series, KAFKA-20193,
KAFKA-19042, KAFKA-19914, MINOR cleanups), CVE fixes in the Docker image
(`23bce3d4ca`), KIP-1035 Streams self-managed offsets, as well as the whole
KAFKA-20245/20410/20415/20549 DLQ complex beyond the wire/config entries
(a purely internal share group DLQ implementation, which Surgewave can derive
independently as soon as `share.version=2` is enabled).

# Kafka trunk 12c81a2e7..46dc4b51f7 (2026-06-22 .. 2026-10-20) — Surgewave relevance

781 commits since the last comparison. Kafka was at `12c81a2e7` (2026-06-22, KAFKA-20624) when the
KIP-1242 and KIP-1319 work was done (Surgewave `7d6dc23`, `b7a67f6`). This review stops at
`46dc4b51f7` (2026-10-20, trunk `4.5.0-SNAPSHOT`; latest tags `4.3.2`, `4.4.0-rc4`, `4.2.2`). The next
comparison starts there.

Assessed only from the point of view of Kafka wire protocol compatibility and broker coordinator
semantics. Client-only, Streams-only and Connect-only KIPs (KIP-909, 1356, 1285, 1340, 1265, 995,
1238, 1332) are out of scope.

## Follow-up

| Upstream | Surgewave today | Ticket |
|---|---|---|
| KAFKA-20785 (`c328f3d97b`), KAFKA-21181 (`9dfd65db3d`, `814cfa34eb`), KAFKA-20357 (`b392ff0322`) — EndTxn answers PRODUCER_FENCED / NOT_COORDINATOR, TV2 accepts a retried completed commit, `lastProducerEpoch` persisted | **not covered** — `EndTxnAsync` answers `InvalidTxnState` for every state but Ongoing/Empty; `TxnErrorStatus` has no `ProducerFenced`, `NotCoordinator` | #190 |
| KAFKA-15591 (`81b993044e`) — first sequence on a never-written partition must be 0 | **not covered** — `ProducerStateManager` accepts any first sequence | #191 |
| KIP-1331 (`be4d31c869`), KIP-1357 (`359763b286`, `89a6c9eda1`), KAFKA-20744 (`b91561ac8a`) — Streams groups v1 stable, API 93, `AssignorName`, `MISSING_CLIENT_TAGS` | **not covered** — Heartbeat/Describe max v0, no API 93 | #192 |
| KIP-1191 share.version 2 production (`ef4b7904be`, `19815b82ce`), KAFKA-20910 (`be72089deb`), share-coordinator fencing (`cde0d5687d`, `9dc859e957`), KAFKA-20783 series | **partial** — `share.version` max 1; AlterOffsets without epoch bump | #193 |
| KIP-1276 (`82b804fe27`, `9c943129eb`) — CIDR host patterns for ACLs | **not covered** — exact host match | #128 |
| KAFKA-21076 (`4338e13ea3`), KAFKA-20898 (`91970ff8c3`) — telemetry; DeleteGroups v3 (`7997c9ebe0`); `metadata.version` 22 vs. 33; KIP-1313 header v3 | small / partial | #194 |

## Protocol schema changes (`clients/src/main/resources/common/message/`)

184 files changed, most of them only gaining the now mandatory `headerVersions` block (KIP-1313).

| Message | Before → now | Change |
|---|---|---|
| `RequestHeader` | 1-2 → 1-3 | v3: tagged `ClientInstanceId` (uuid, tag 0). Surgewave skips unknown header tagged fields, so v3 parses. |
| `OffsetDelete` | 0 → 0-1 (unstable) | v1 flexible, header v3 |
| `GetTelemetrySubscriptions`, `PushTelemetry` | 0 → 0-1 (unstable) | v1 drops `ClientInstanceId` from the body (it is in the header) |
| `ApiVersionsRequest` | 0-5 | v5 no longer unstable (KIP-1242, `7be741d08b`) — covered |
| `StreamsGroupHeartbeat` | 0-1 | v1 stable; status `MISSING_CLIENT_TAGS` |
| `StreamsGroupDescribe` | 0-1 | v1 stable; `AssignorName` |
| `StreamsGroupTopologyDescriptionUpdate` (93) | 0 | stable |
| `UnregisterController` (94) | new | KAFKA-20395 (`c274a7348f`), MetadataVersion `IBP_4_4_IV2` |

## Not applicable to Surgewave

- KRaft voter fixes KAFKA-20865 (`05f61b9ff5`), KAFKA-20661 (`7565048a5c`) and `UnregisterController`
  (94): Surgewave's metadata quorum is its own implementation, not KRaft's wire.
- KAFKA-20601 (`e6919341bb`), static member rejoining a full group: Surgewave enforces no group
  max size.
- KIP-1279 cluster mirroring: configs only, marked internal upstream.
- KIP-1232 (`broker.id` deprecated, `44e45841cc`): Surgewave's broker identity is its own config.

## Worth a look, no ticket yet

- Consumer groups: KAFKA-20662 (`ec004fd80f`, INCONSISTENT_GROUP_PROTOCOL during online migration),
  KAFKA-20747 (`71faeec8b0`) and `3e5df2f083` (static instance id), KAFKA-20938 (`32dd104fed`, stale
  JoinGroup/SyncGroup futures).
- Storage: KAFKA-17375 (`b1d74d74df`, follower appends larger than `segment.bytes`), KAFKA-16304
  (`0d82450b0a`, log dir offline on a disk error in zero-copy fetch), fsync on shutdown
  (`c0c809772f`, `18cdc642d8`).
- Security: KAFKA-20774 (`14cde029c5`, `ScramDelta` dropping users).

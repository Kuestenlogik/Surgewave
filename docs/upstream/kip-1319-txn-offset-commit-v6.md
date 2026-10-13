# KIP-1319 / TxnOffsetCommit v6 — Implementation plan

**Status:** Deferred. Kafka itself has marked v6 as `latestVersionUnstable: true`;
clients cannot use the version at the moment. Surgewave
should follow up as soon as Kafka releases v6 as stable (probably 4.4
or 4.5).

**Kafka refs:** `7562044781`, `2342c80dca`, `baa064e422`, `319dd61cb3`, `7f5861817d`.

## Wire schema changes

### TxnOffsetCommitRequest

- `validVersions`: `0-5` -> `0-6` with `latestVersionUnstable: true`.
- Top level: `GenerationId` -> `GenerationIdOrMemberEpoch` (source code rename;
  wire-positional unchanged).
- `Topics[].Name`: `versions: "0-5"`, `ignorable: true` (was "0+").
- `Topics[].TopicId` NEW: `type: uuid`, `versions: "6+"`, `ignorable: true`.

### TxnOffsetCommitResponse

- `validVersions`: `0-5` -> `0-6`.
- `Topics[].Name`: `versions: "0-5"`, `ignorable: true`.
- `Topics[].TopicId` NEW: `type: uuid`, `versions: "6+"`, `ignorable: true`.
- New error codes in v6+: `GROUP_ID_NOT_FOUND` (69), `STALE_MEMBER_EPOCH`
  (113), `UNKNOWN_TOPIC_ID` (100).

## Surgewave changes per patch

### Patch 1 / `7562044781` — prepare the schema

1. **TxnOffsetCommitRequest.Topics refactor**:
   - currently: `Dictionary<string, List<TxnOffsetCommitPartition>>`
   - new: `List<TxnOffsetCommitTopic>` with a wrapper class:
     ```csharp
     public sealed class TxnOffsetCommitTopic
     {
         public string? Name { get; init; }        // v0-5
         public Guid? TopicId { get; init; }       // v6+
         public required List<TxnOffsetCommitPartition> Partitions { get; init; }
     }
     ```
2. **TxnOffsetCommitResponse**: likewise.
3. **Caller adjustments** (4+ files):
   - `Kuestenlogik.Surgewave.Api.Grpc.Server/TransactionServiceImpl.cs`
   - `Kuestenlogik.Surgewave.Broker/Native/Operations/Transactions/TransactionOperations.cs`
   - `Kuestenlogik.Surgewave.Broker/TransactionCoordinator.cs`
   - `Kuestenlogik.Surgewave.Broker/Program.cs`

   Every caller that currently does `request.Topics[topic]` must now use a
   `List` lookup helper method (e.g. `request.FindTopic(name)` /
   `FindTopicById(id)`).

### Patch 3 / `2342c80dca` — builder validation

- Java validates: v6+ requires TopicId, v0-5 requires Name.
- Surgewave equivalent: validation in `WriteTo` / `ReadFrom` (or in the
  coordinator handler).
- Throw `InvalidRequestException` (or a comparable ApiException) when a
  v6 request does not send a TopicId or a v0-5 request does not send a Name.

### Patch 4 / `baa064e422` — the response builder carries the TopicId through

- Building the `TxnOffsetCommitResponse` must pass the TopicId from the request
  through into the response for v6+ (also for error answers).
- `getErrorResponse(ApiException)` as a helper, analogous to Java.

### Patch 5 / `319dd61cb3` — server handler name<->TopicId resolution

- In the Surgewave coordinator path (probably `TransactionCoordinator.HandleTxnOffsetCommit`):
  - v6+: TopicId present -> `LogManager.GetTopicMetadataById(topicId)` ->
    if null: `UNKNOWN_TOPIC_ID`; otherwise the topic name for internal processing.
  - v0-5: TopicName present -> `LogManager.GetTopicMetadata(name)` -> TopicId
    for internal processing (consistent bookkeeping).
- `LogManager` may need a `GetTopicMetadataById(Guid)` method, if it
  does not exist yet.

### Patch 6 / `7f5861817d` — client error mapping (deferred)

- Surgewave currently has no .NET tx client of its own with its own
  TransactionManager logic. If `Kuestenlogik.Surgewave.Client.Native`
  ever gets tx support, its error handling must treat
  `GROUP_ID_NOT_FOUND` / `STALE_MEMBER_EPOCH` in addition to
  `ILLEGAL_GENERATION` / `UNKNOWN_MEMBER_ID` as a group metadata mismatch
  (equivalent of `CommitFailedException`).

## ApiVersions

As long as v6 is `latestVersionUnstable: true` in Kafka:

- `Kuestenlogik.Surgewave.Protocol.Kafka/Requests/ApiVersionsRequest.cs` line 285:
  **keep** `TxnOffsetCommit MaxVersion = 5`. Only raise it to 6 when
  Kafka removes the marker.

## Sibling repos

None. TxnOffsetCommit is KRaft/broker-internal — the connectors do not speak
it directly; Confluent.Kafka as a test client can keep using v0-5.

## Effort

- Patch 1 (schema refactor + caller adjustment): ~1-2h
- Patch 3 (validation): ~15min
- Patch 4 (response builder): ~30min
- Patch 5 (server handler + LogManager extension): ~1-2h
- Patch 6 (client): deferred until a native tx client exists
- Build + smoke test: ~30min

**Total ~3-5h for a session of its own.** Before implementing, check
whether Kafka has removed the `latestVersionUnstable` marker.

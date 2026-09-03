# KIP-1319 / TxnOffsetCommit v6 — Implementierungsplan

**Status:** Deferred. Kafka hat v6 selbst als `latestVersionUnstable: true`
markiert; Clients koennen die Version aktuell nicht nutzen. Surgewave
sollte nachziehen, sobald Kafka v6 als stable freigibt (vermutlich 4.4
oder 4.5).

**Kafka-Refs:** `7562044781`, `2342c80dca`, `baa064e422`, `319dd61cb3`, `7f5861817d`.

## Wire-Schema-Aenderungen

### TxnOffsetCommitRequest

- `validVersions`: `0-5` -> `0-6` mit `latestVersionUnstable: true`.
- Top-Level: `GenerationId` -> `GenerationIdOrMemberEpoch` (Quellcode-Rename;
  wire-positional unveraendert).
- `Topics[].Name`: `versions: "0-5"`, `ignorable: true` (war "0+").
- `Topics[].TopicId` NEU: `type: uuid`, `versions: "6+"`, `ignorable: true`.

### TxnOffsetCommitResponse

- `validVersions`: `0-5` -> `0-6`.
- `Topics[].Name`: `versions: "0-5"`, `ignorable: true`.
- `Topics[].TopicId` NEU: `type: uuid`, `versions: "6+"`, `ignorable: true`.
- Neue Error-Codes in v6+: `GROUP_ID_NOT_FOUND` (69), `STALE_MEMBER_EPOCH`
  (113), `UNKNOWN_TOPIC_ID` (100).

## Surgewave-Aenderungen pro Patch

### Patch 1 / `7562044781` — Schema vorbereiten

1. **TxnOffsetCommitRequest.Topics-Refactor**:
   - aktuell: `Dictionary<string, List<TxnOffsetCommitPartition>>`
   - neu: `List<TxnOffsetCommitTopic>` mit Wrapper-Klasse:
     ```csharp
     public sealed class TxnOffsetCommitTopic
     {
         public string? Name { get; init; }        // v0-5
         public Guid? TopicId { get; init; }       // v6+
         public required List<TxnOffsetCommitPartition> Partitions { get; init; }
     }
     ```
2. **TxnOffsetCommitResponse**: dito.
3. **Caller-Anpassungen** (4+ Files):
   - `Kuestenlogik.Surgewave.Api.Grpc.Server/TransactionServiceImpl.cs`
   - `Kuestenlogik.Surgewave.Broker/Native/Operations/Transactions/TransactionOperations.cs`
   - `Kuestenlogik.Surgewave.Broker/TransactionCoordinator.cs`
   - `Kuestenlogik.Surgewave.Broker/Program.cs`

   Jeder Caller, der bisher `request.Topics[topic]` macht, muss jetzt eine
   `List`-Lookup-Helper-Methode benutzen (z.B. `request.FindTopic(name)` /
   `FindTopicById(id)`).

### Patch 3 / `2342c80dca` — Builder-Validation

- Java validiert: v6+ erfordert TopicId, v0-5 erfordert Name.
- Surgewave-Aequivalent: Validierung im `WriteTo` / `ReadFrom` (oder im
  Coordinator-Handler).
- `InvalidRequestException` (oder vergleichbare ApiException) werfen, wenn
  v6-Request keinen TopicId mitschickt bzw. v0-5-Request keinen Name.

### Patch 4 / `baa064e422` — Response-Builder traegt TopicId durch

- `TxnOffsetCommitResponse`-Erzeugung muss bei v6+ TopicId aus dem Request
  in die Response durchreichen (auch bei Error-Antworten).
- `getErrorResponse(ApiException)` als Helper analog Java.

### Patch 5 / `319dd61cb3` — Server-Handler Name<->TopicId-Aufloesung

- Im Surgewave-Coordinator-Pfad (vermutlich `TransactionCoordinator.HandleTxnOffsetCommit`):
  - v6+: TopicId vorhanden -> `LogManager.GetTopicMetadataById(topicId)` ->
    falls null: `UNKNOWN_TOPIC_ID`; sonst Topic-Name fuer interne Verarbeitung.
  - v0-5: TopicName vorhanden -> `LogManager.GetTopicMetadata(name)` -> TopicId
    fuer interne Verarbeitung (Bookkeeping konsistent).
- `LogManager` braucht ggf. eine `GetTopicMetadataById(Guid)`-Methode, falls
  noch nicht vorhanden.

### Patch 6 / `7f5861817d` — Client-Error-Mapping (deferred)

- Surgewave hat aktuell keinen eigenen .NET-Tx-Client mit eigener
  TransactionManager-Logik. Wenn der `Kuestenlogik.Surgewave.Client.Native`
  jemals Tx-Support bekommt, muss die Error-Behandlung
  `GROUP_ID_NOT_FOUND` / `STALE_MEMBER_EPOCH` zusaetzlich zu
  `ILLEGAL_GENERATION` / `UNKNOWN_MEMBER_ID` als Group-Metadata-Mismatch
  behandeln (`CommitFailedException` aequivalent).

## ApiVersions

Solange v6 in Kafka `latestVersionUnstable: true` ist:

- `Kuestenlogik.Surgewave.Protocol.Kafka/Requests/ApiVersionsRequest.cs` Zeile 285:
  `TxnOffsetCommit MaxVersion = 5` **belassen**. Erst auf 6 erhoehen, wenn
  Kafka den Marker entfernt.

## Sibling-Repos

Keine. TxnOffsetCommit ist KRaft/Broker-internal — die Connectors sprechen
es nicht direkt; Confluent.Kafka als Test-Client kann v0-5 weiter benutzen.

## Aufwand

- Patch 1 (Schema-Refactor + Caller-Anpassung): ~1-2h
- Patch 3 (Validation): ~15min
- Patch 4 (Response-Builder): ~30min
- Patch 5 (Server-Handler + LogManager-Erweiterung): ~1-2h
- Patch 6 (Client): deferred bis Native-Tx-Client existiert
- Build + Smoke-Test: ~30min

**Gesamt ~3-5h fuer eine eigene Session.** Vor dem Implementieren pruefen
ob Kafka den `latestVersionUnstable`-Marker entfernt hat.

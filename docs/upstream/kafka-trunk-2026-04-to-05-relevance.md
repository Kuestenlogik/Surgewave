# Kafka trunk 5d03ccff57..HEAD (2026-04-04 .. 2026-05-11) — Surgewave-Relevanz

193 Commits gesichtet. 26 als Surgewave-relevant eingestuft. Bewertung **nur** aus
Sicht der Kafka-Wire-Protokoll-Kompatibilitaet und Broker-Coordinator-Semantik —
Connect-/Streams-/Tools-/Test-/Build-Aenderungen sind ausgeklammert (siehe
Out-of-Scope unten).

## Surgewave-relevante Commits

Sortiert: high → medium → low.

| Commit | KAFKA-Ticket / KIP | Kategorie | Surgewave-Relevanz | Was nachziehen |
|---|---|---|---|---|
| 7562044781 | KAFKA-20444 [1/N] / KIP-1319 | wire-protocol | **high** — neues TxnOffsetCommit v6 fuegt `TopicId` ein, bindet `Name` an v0-5; benennt `GenerationId` -> `GenerationIdOrMemberEpoch` (semantisch). Coordinator wartet jetzt auf MemberEpoch unter neuem Consumer-Group-Protokoll. | TxnOffsetCommitRequest/Response v6 in Surgewave-Wire-Schema aufnehmen. v6 noch als unstable lassen bis Folgepatches durch sind. |
| 2342c80dca | KAFKA-20444 [3/N] / KIP-1319 | wire-protocol | **high** — Builder validiert: v6+ erfordert TopicId, v0-5 TopicName. | Analoge Builder-Logik in Surgewave-Producer-Code, falls eigener Native-Client TxnOffsetCommit erzeugt. |
| baa064e422 | KAFKA-20444 [4/N] / KIP-1319 | wire-protocol | **high** — TxnOffsetCommitResponse fuer TopicIds; `getErrorResponse` traegt TopicId durch. | Response-Builder im Surgewave-Coordinator nachziehen, falls schon TxnOffsetCommit implementiert. |
| 319dd61cb3 | KAFKA-20444 [5/N] / KIP-1319 | wire-protocol+coordinator | **high** — KafkaApis loest fuer v6+ TopicName aus MetadataCache via TopicId auf; v0-5 ruckwaerts: TopicId via Name. Bei Fehlschlag `UNKNOWN_TOPIC_ID`. | Handler in Surgewave's TxnOffsetCommit-Pfad ergaenzen: bidirektionale Aufloesung Name<->ID + Fehlercodes. |
| 7f5861817d | KAFKA-20444 [6/N] / KIP-1319 | wire-protocol+client | **high** — TransactionManager-Client behandelt jetzt `GROUP_ID_NOT_FOUND` und `STALE_MEMBER_EPOCH` zusaetzlich zu `ILLEGAL_GENERATION`/`UNKNOWN_MEMBER_ID` als Group-Metadata-Mismatch (CommitFailedException). | Falls Surgewave einen eigenen Producer-Native-Client mit Tx-Support hat: Errorbehandlung fuer v6 anpassen. Server-seitig sicherstellen, dass Coordinator diese Codes zurueckgibt. |
| 6be48c6e54 | KAFKA-20441 | wire-protocol+kraft | **high** — entfernt `CordonedLogDirs` aus BrokerRegistrationRequest v5; bringt es stattdessen als nullable tagged field in BrokerHeartbeatRequest v2 (tag 1). `validVersions` faellt von 0-5 auf 0-4 zurueck. **Breaking** ggue. einer Implementierung, die schon v5 sprach. | BrokerRegistrationRequest/Response v5 in Surgewave wieder entfernen; BrokerHeartbeatRequest v2 `CordonedLogDirs` als nullable behandeln (null bevor Broker RECOVERY-State erreicht hat). |
| 52687216c9 | KAFKA-20442 | coordinator | **high** — Consumer-Group-Bug: bei Fence aller Member wird `groupEpoch` gebumpt, aber `assignmentEpoch` nicht — verletzt Invariante `empty group => groupEpoch == assignmentEpoch`. Fix schreibt `ConsumerGroupTargetAssignmentMetadataRecord` mit Timestamp 0. | Surgewave's GroupCoordinator pruefen: gleichen Pfad bei Member-Fence implementieren, Target-Assignment-Epoch nachziehen. |
| 56be35588a | KAFKA-20442 | coordinator | **high** — selbes Problem fuer ShareGroups (`ShareGroupTargetAssignmentMetadataRecord`). | Analog in Surgewave's ShareGroup-Coordinator. |
| e08a15091d | KAFKA-20442 | coordinator | **high** — selbes Problem fuer StreamsGroups (`StreamsGroupTargetAssignmentMetadataRecord`). | Analog in Surgewave's StreamsGroup-Coordinator (falls implementiert). |
| 0d9fe518b6 | KAFKA-20434 | coordinator | **high** — Consumer-Group recomputed Assignment nicht, wenn alle Static-Member mit anderem Server-Side-Assignor rejoinen. `bumpGroupEpoch` ignoriert preferred-assignor-Wechsel. | Surgewave's group-epoch-bump pruefen: muss Wechsel des effektiven preferred server-assignor erkennen (mit Default-Fallback). |
| d5d9868568 | KAFKA-20431 | wire-protocol+coordinator | **high** — ConsumerGroupDescribe-Response: `Assignment.assignedPartitions` enthielt nicht `partitionsPendingRevocation` waehrend Reconciliation — Partitions "verschwinden" sichtbar fuer Clients. Fix mergt beide. | Surgewave's ConsumerGroupDescribe-Handler pruefen: `Assignment` muss `assignedPartitions ∪ partitionsPendingRevocation` melden. |
| 06f699664b | KAFKA-20415 / KIP-1191 | wire-protocol+coordinator | **medium** — neue Feature-Version `share.version=2` (gated DLQ-Support), neue MetadataVersion `IBP_4_4_IV0`. ApiVersions advertised jetzt SV_2 max. | Surgewave's ApiVersionsResponse-Feature-Advertising aktualisieren (ShareVersion 0/1/2). DLQ-Server-Logik selber ist KIP-1191-spezifisch, kann zunaechst SV_1 bleiben. |
| b6d2503710 | KAFKA-20410 / KIP-1191 | coordinator+config | **medium** — neue Group-Configs fuer Share-Group-DLQ: `errors.deadletterqueue.auto.create.topics.enable`, `errors.deadletterqueue.replication.factor`, `errors.deadletterqueue.max.delivery.attempts`. | Surgewave's GroupCoordinatorConfig + ShareGroupConfigs erweitern (defensive: akzeptieren + ignorieren, wenn DLQ nicht implementiert). |
| cb7e3ab375 | KAFKA-20410 [2/N] / KIP-1191 | config | **medium** — neue TopicConfig: `ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG` in LogConfig registriert. | LogConfig in Surgewave's Storage-Engine analog erweitern (toleranter Default). |
| faa8b4870f | KAFKA-20533 | coordinator+wire | **medium** — ShareFetch: bei Topic-Loeschung war Error-Code im Response falsch (`UNKNOWN_SERVER_ERROR` statt korrektem Per-Partition-Code wegen NPE bei Topic-Name=null). | Surgewave's ShareFetch-Handler pruefen: bei Topic-Loeschung muss korrekter Per-Partition-Error-Code zurueck (kein NPE, kein Generic-Fehler). |
| 44bafc60e7 | KAFKA-20426 | client-bug | **medium** — AsyncKafkaConsumer Busy-Loop, wenn `group.id` + `assign()` kombiniert: `maximumTimeToWait`=0 wenn Heartbeat geskippt. Wire nicht direkt sichtbar, aber Client-DDoS-Vektor. | Surgewave's Native-.NET-Consumer (falls vorhanden): pruefen, ob bei Manual-Assignment + group.id keine Busy-Loop entsteht. |
| 4b9eddc132 | KAFKA-20428 | client-bug | **medium** — unsubscribe applied versehentlich pending Assignment-Updates aus Background-Queue. | Native-Client-Logik pruefen, falls implementiert. |
| b954b35d0a | KAFKA-20332 | client-bug | **medium** — Race: App-Thread konnte Records fuer revoked Partitions liefern, weil revoke nicht synchronisiert war. Wire-sichtbarer Effekt: Consumer commitet Offset fuer fremde Partition. | Native-Client / Consumer-Reconciliation-Logik pruefen. |
| 7e1c9db92f | KAFKA-20332 [2] | client-bug | low — Wakeup-Handling im Poll-Reconciliation-Check. | Folgepatch zu obigem, nur bei eigener AsyncConsumer-Impl. |
| 0749b320ba (siehe Hinweis) | — | (Streams-only) | low | (nicht relevant) |
| d0e0ec478c | KAFKA-20312 | client-bug | low — NPE in `OffsetFetcherUtils.regroupPartitionMapByNode` bei null-Leader waehrend Metadata-Race. | Defensive Null-Check uebernehmen, falls eigener Offset-Fetcher. |
| b033b2e8f4 | KAFKA-20114 | client-bug | low — Race in `RPCProducerIDManager` zwischen `maybeRequestNextBlock()` und `handleUnsuccessfulResponse()` — verfruehter Retry. | Defensiv pruefen, wenn Surgewave einen eigenen ProducerID-Manager-Client hat. |
| 123ee9e45d | KAFKA-20393 | client-bug | low — `TelemetrySender.stickyNode` benutzte stale IP nach Broker-Address-Change (K8s pod replacement). | Telemetry-Client pruefen, falls implementiert. |
| 86328c25ee | KAFKA-19566 / KIP-1162 | quota | low — `ClientQuotaCallback#updateClusterMetadata` deprecated. In 5.0 wird sie entfernt. Verhalten zur Laufzeit unveraendert. | Hinweis vermerken, falls Surgewave Quota-Callback exponiert. |
| ff2ba93a5c | KAFKA-13022 | quota | low — `ClientQuotasImage#describe` Perf-Optimierung via Index-Map. Reine Server-side-Optimierung. | Surgewave's Quota-Image evtl. analog optimieren, falls Perf-Hotspot. |
| 22c1e445f1 | — | coordinator+config | low — bessere Fehlermeldungen mit numerischen Bounds in GroupConfig-Validierung. | Surgewave's GroupConfig-Validator analog verbessern (UX). |
| e4cd243abc | KAFKA-20337 | coordinator | low — Refactoring: alle `GroupConfig`-Felder werden `Optional<T>`, Broker-Defaults werden lazy aufgeloest — Fix gegen stale-capture bei dynamischen Broker-Config-Aenderungen. | Surgewave's GroupConfig pruefen, ob aehnliche Stale-Capture-Issue existiert. |
| 72e380c149 | KAFKA-20380 | kraft+config | low — Controller leitet `advertised.listeners` aus `controller.quorum.voters` ab, wenn nicht definiert. Wire-Effekt nur fuer KRaft-Bootstrap. | Surgewave's Controller-Listener-Resolution-Logik pruefen (vermutlich anders implementiert). |

## Empfohlene Folge-Tickets fuer Surgewave

1. **KIP-1319 / TxnOffsetCommit v6 nachziehen** — Top-Prioritaet, da 6 Patches in
   Folge gemerged wurden. Wire-Schema (`TxnOffsetCommitRequest.json`,
   `TxnOffsetCommitResponse.json`) + Server-Handler (Name<->TopicId-Aufloesung
   inkl. `UNKNOWN_TOPIC_ID`-Fehler) + Client-Error-Mapping fuer
   `GROUP_ID_NOT_FOUND` / `STALE_MEMBER_EPOCH`. Kafka-Refs:
   `7562044781`, `2342c80dca`, `baa064e422`, `319dd61cb3`, `7f5861817d`.
2. **GroupCoordinator: Assignment-Epoch beim Fence-aller-Member nachziehen** —
   in Consumer-Group, ShareGroup, StreamsGroup. Bei Surgewave eigene
   Implementierung pruefen, gegebenenfalls denselben Bug fixen. Kafka-Refs:
   `52687216c9`, `56be35588a`, `e08a15091d`.
3. **ConsumerGroupDescribe-Response: Pending-Revocation-Partitions mitschicken** —
   sonst "verschwinden" Partitions waehrend Reconciliation aus Sicht von
   Admin-Tools. Kafka-Ref: `d5d9868568`.
4. **BrokerHeartbeat v2: `CordonedLogDirs` als nullable tagged field; BrokerRegistration v5 NICHT
   implementieren** (wurde zurueckgenommen). Surgewave sollte das v5-Feld nicht
   einfuehren, sondern nur die HeartbeatRequest-Variante. Kafka-Ref: `6be48c6e54`.
5. **`share.version=2` Feature-Level einfuehren** — auch ohne volle DLQ-Implementierung
   muss ApiVersionsResponse SV_2 als max melden (sobald DLQ-Server-Logik geplant
   ist). Bis dahin bewusst SV_1 lassen. Kafka-Ref: `06f699664b`.
6. **ShareFetch-Error-Mapping bei Topic-Loeschung** — kein NPE, korrekter
   Per-Partition-Error-Code statt `UNKNOWN_SERVER_ERROR`. Kafka-Ref: `faa8b4870f`.
7. **Consumer-Group `bumpGroupEpoch` muss preferred-server-assignor-Wechsel
   erkennen** — Edge-Case bei Static-Member-Rejoin mit anderem Assignor.
   Kafka-Ref: `0d9fe518b6`.

## Umsetzungs-Status (Sprint 2026-05-11)

| Item | Status | Commit/Doc |
|---|---|---|
| KIP-1319 / TxnOffsetCommit v6 | **deferred** — Kafka selbst `latestVersionUnstable: true` | `concept/kip-1319-txn-offset-commit-v6.md` |
| KAFKA-20441 / BrokerHeartbeat v2 | **done** — tagged-field-Refactor + nullable CordonedLogDirs | `8f25f57` |
| KAFKA-20442 / Assignment-Epoch | **done** — verified, Surgewave's Modell trivial gleich | `432a918` |
| KAFKA-20434 / bumpGroupEpoch Assignor | **done** — verified, by-design strenger als Java | `b62e549` |
| KAFKA-20431 / ConsumerGroupDescribe pending-rev | **done** — Vereinigung Assignment + Owned | `dfe03e0` |
| KAFKA-20415 / share.version=1 | **done** — ApiVersionsResponse erweitert; SV_2 wartet auf DLQ | `d5c7cd1` |
| KAFKA-20410 / DLQ Group-Configs | **noop** — Surgewave hat keinen Group-Resource-Config-Support; bei Implementierung mitziehen | (kein commit) |

## Out-of-Scope (bewusst nicht nachgezogen)

Connect-/MirrorMaker-interne Aenderungen (`dc5f816403`, `eef6cab648`,
`9c106dc9d0`), Streams-DSL/State-Store-Aenderungen (`94b6886b12`, `0749b320ba`,
KAFKA-20194/20329/20173/20307/20422 Header-Stores, KAFKA-20396/20398/20456/20264/20194 etc.),
ConfigCommand-/Tools-/Docs-/Test-Refactorings (KAFKA-20297-Serie, KAFKA-20193,
KAFKA-19042, KAFKA-19914, MINOR-Cleanups), CVE-Fixes im Docker-Image
(`23bce3d4ca`), KIP-1035-Streams-Self-Managed-Offsets, sowie der gesamte
KAFKA-20245/20410/20415/20549-DLQ-Komplex jenseits der Wire-/Config-Eintragungen
(rein interne Share-Group-DLQ-Implementierung, die Surgewave eigenstaendig
ableiten kann sobald `share.version=2` aktiviert wird).

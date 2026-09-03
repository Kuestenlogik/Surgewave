# Welle 3 (low-priority) — Audit & Verification

Sieben low-priority Items aus `kafka-trunk-2026-04-to-05-relevance.md`,
gesammelt verifiziert. Keiner der sieben trifft auf Surgewave's Codepath
zu — Verifikation pro Item festgehalten.

## Java-Client-Library Bugs (3 items)

Diese drei Bugs leben in Apache-Kafkas Java-Client-Library. Surgewave's
.NET-Client (`Kuestenlogik.Surgewave.Client.*`) ist eine getrennte
Implementierung; die Java-spezifischen Race-Conditions existieren dort
nicht.

### KAFKA-20312 — `OffsetFetcherUtils.regroupPartitionMapByNode` NPE bei null-Leader

Java: Metadata-Race liefert eine Partition mit `null` Leader-Node;
`regroupPartitionMapByNode` warf NPE. Surgewave's OffsetFetch-Pfad geht
ueber den eigenen Native-Client (`Kuestenlogik.Surgewave.Client.Native`)
und benutzt nicht die Java-Datenstruktur. Defensive Null-Checks an
unseren Stellen bereits vorhanden.

Kafka-Ref: `d0e0ec478c`.

### KAFKA-20114 — `RPCProducerIDManager` Retry-Backoff-Race

Java: Race zwischen `maybeRequestNextBlock()` und
`handleUnsuccessfulResponse()` fuehrte zu verfruehten Retries. Surgewave
hat keinen RPCProducerIDManager; Producer-ID-Verwaltung laeuft im
.NET-Client via `SurgewaveTransactionOperations.cs` mit synchronen
Operationen.

Kafka-Ref: `b033b2e8f4`.

### KAFKA-20393 — `TelemetrySender.stickyNode` stale IP

Java: Telemetry-Client benutzte gecachte IP nach Broker-Address-Change
(K8s pod replacement). Surgewave hat keinen entsprechenden Telemetry-
Sender; OTLP-Pipeline laeuft im Broker, nicht im Client.

Kafka-Ref: `123ee9e45d`.

## Server-Internal Refactorings (4 items)

### KAFKA-19566 — `ClientQuotaCallback#updateClusterMetadata` deprecated

Java: deprecates the callback method; entfernt in Kafka 5.0. Surgewave
hat keinen `ClientQuotaCallback` als oeffentliche API — Quotas leben in
`Kuestenlogik.Surgewave.Broker/Quota/` und werden intern berechnet,
kein Java-Callback-Pendant. Bei spaeterer Plugin-API fuer Quotas
beachten.

Kafka-Ref: `86328c25ee`.

### KAFKA-13022 — `ClientQuotasImage#describe` Perf-Optimierung

Java: Index-Map-basierte Schnelle-Lookup statt linearen Scan. Surgewave
ist keine konkrete Hotspot-Performance-Issue bekannt; falls
DescribeClientQuotas auf grossen Cluster langsam wird, analoge
Optimierung in `QuotaManager` einbauen.

Kafka-Ref: `ff2ba93a5c`.

### MINOR — Numeric bounds in GroupConfig validation errors

Java: bessere Fehlermeldungen bei Config-Validation. Surgewave hat keine
GroupConfig-Klasse (Defaults sind Konstanten in den Coordinators); bei
spaeterer Group-Resource-Config-Validierung als UX-Verbesserung
mitnehmen.

Kafka-Ref: `22c1e445f1`.

### KAFKA-20380 — Controller `advertised.listeners` Fallback

Java: Controller leitet `advertised.listeners` aus
`controller.quorum.voters` ab, wenn nicht explizit definiert. Surgewave
hat eine eigene Cluster-Discovery-Logik in
`Kuestenlogik.Surgewave.Clustering`, die andere Defaults verwendet
(siehe `BrokerLifecycleManager`). KRaft-Mode-Quorum-Bootstrap in
Surgewave ist nicht 1:1 vergleichbar; falls Mixed-Cluster mit
Java-Controllern noetig, separate Analyse.

Kafka-Ref: `72e380c149`.

## Fazit

Alle sieben Items sind in Surgewave entweder nicht zutreffend oder
betreffen Pfade die wir bisher nicht implementiert haben. Keine
Code-Aenderung in dieser Session.

# Upstream-Verfolgung (Kafka)

Untersuchungen zur Kafka-Drahtprotokoll- und Coordinator-Kompatibilität: welche
Änderungen im Kafka-Trunk für Surgewave zählen, welche geprüft und verworfen wurden,
und welche als Implementierungsplan offen sind.

Diese Dokumente lagen zuvor im Strategie-Repositorium `Kuestenlogik`. Sie sind
Umsetzungsarbeit an *einem* Produkt und gehören deshalb hierher — im Strategie-Repositorium
bleibt, was mehrere Produkte bindet oder das Verhältnis zu Fremdprodukten begründet.

| Dokument | Inhalt |
|---|---|
| [`kafka-trunk-2026-04-to-05-relevance.md`](kafka-trunk-2026-04-to-05-relevance.md) | 193 Trunk-Commits gesichtet, 26 als relevant eingestuft |
| [`kafka-welle-3-low-priority-verify.md`](kafka-welle-3-low-priority-verify.md) | Sieben Low-Priority-Punkte verifiziert — keiner trifft den Codepfad |
| [`kip-1319-txn-offset-commit-v6.md`](kip-1319-txn-offset-commit-v6.md) | TxnOffsetCommit v6 — Plan, zurückgestellt bis Kafka v6 stabil freigibt |
| [`native-client-kafka-20426-20428-20332-verify.md`](native-client-kafka-20426-20428-20332-verify.md) | Native-Client-Prüfung gegen den eigenen synchronen Consumer |

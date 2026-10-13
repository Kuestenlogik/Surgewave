# Wave 3 (low-priority) — Audit & Verification

Seven low-priority items from `kafka-trunk-2026-04-to-05-relevance.md`,
verified together. None of the seven applies to Surgewave's code path
— the verification is recorded per item.

## Java client library bugs (3 items)

These three bugs live in Apache Kafka's Java client library. Surgewave's
.NET client (`Kuestenlogik.Surgewave.Client.*`) is a separate
implementation; the Java-specific race conditions do not exist there.

### KAFKA-20312 — `OffsetFetcherUtils.regroupPartitionMapByNode` NPE on a null leader

Java: a metadata race returns a partition with a `null` leader node;
`regroupPartitionMapByNode` threw an NPE. Surgewave's OffsetFetch path goes
through our own native client (`Kuestenlogik.Surgewave.Client.Native`)
and does not use the Java data structure. Defensive null checks are
already in place at our call sites.

Kafka ref: `d0e0ec478c`.

### KAFKA-20114 — `RPCProducerIDManager` retry backoff race

Java: a race between `maybeRequestNextBlock()` and
`handleUnsuccessfulResponse()` led to premature retries. Surgewave
has no RPCProducerIDManager; producer ID management runs in the
.NET client via `SurgewaveTransactionOperations.cs` with synchronous
operations.

Kafka ref: `b033b2e8f4`.

### KAFKA-20393 — `TelemetrySender.stickyNode` stale IP

Java: the telemetry client used a cached IP after a broker address change
(K8s pod replacement). Surgewave has no corresponding telemetry
sender; the OTLP pipeline runs in the broker, not in the client.

Kafka ref: `123ee9e45d`.

## Server-internal refactorings (4 items)

### KAFKA-19566 — `ClientQuotaCallback#updateClusterMetadata` deprecated

Java: deprecates the callback method; removed in Kafka 5.0. Surgewave
has no `ClientQuotaCallback` as a public API — quotas live in
`Kuestenlogik.Surgewave.Broker/Quota/` and are computed internally,
with no Java callback counterpart. Keep in mind for a later plugin API
for quotas.

Kafka ref: `86328c25ee`.

### KAFKA-13022 — `ClientQuotasImage#describe` perf optimisation

Java: an index-map-based fast lookup instead of a linear scan. No concrete
hotspot performance issue is known for Surgewave; if
DescribeClientQuotas becomes slow on large clusters, build an analogous
optimisation into `QuotaManager`.

Kafka ref: `ff2ba93a5c`.

### MINOR — Numeric bounds in GroupConfig validation errors

Java: better error messages for config validation. Surgewave has no
GroupConfig class (defaults are constants in the coordinators); take it
along as a UX improvement when group resource config validation comes
later.

Kafka ref: `22c1e445f1`.

### KAFKA-20380 — Controller `advertised.listeners` fallback

Java: the controller derives `advertised.listeners` from
`controller.quorum.voters` when not explicitly defined. Surgewave
has its own cluster discovery logic in
`Kuestenlogik.Surgewave.Clustering`, which uses different defaults
(see `BrokerLifecycleManager`). KRaft-mode quorum bootstrap in
Surgewave is not directly comparable; if mixed clusters with
Java controllers become necessary, a separate analysis is needed.

Kafka ref: `72e380c149`.

## Conclusion

All seven items are either not applicable to Surgewave or concern
paths we have not implemented so far. No code change in this session.

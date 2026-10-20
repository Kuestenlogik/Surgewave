# Upstream tracking (Kafka)

Investigations into Kafka wire protocol and coordinator compatibility: which
changes in the Kafka trunk matter for Surgewave, which were checked and discarded,
and which are open as an implementation plan.

| Document | Content |
|---|---|
| [`kafka-trunk-2026-04-to-05-relevance.md`](kafka-trunk-2026-04-to-05-relevance.md) | 193 trunk commits reviewed, 26 classified as relevant |
| [`kafka-trunk-2026-06-to-10-relevance.md`](kafka-trunk-2026-06-to-10-relevance.md) | 781 trunk commits since `12c81a2e7`, up to `46dc4b51f7` (2026-10-20); follow-ups #190–#194, #128 |
| [`kafka-wave-3-low-priority-verify.md`](kafka-wave-3-low-priority-verify.md) | Seven low-priority items verified — none hits the code path |
| [`kip-1319-txn-offset-commit-v6.md`](kip-1319-txn-offset-commit-v6.md) | TxnOffsetCommit v6 — plan, deferred until Kafka releases v6 as stable |
| [`native-client-kafka-20426-20428-20332-verify.md`](native-client-kafka-20426-20428-20332-verify.md) | Native client check against our own synchronous consumer |

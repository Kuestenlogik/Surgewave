# Native-Client KAFKA-20426/20428/20332 — Verification

Surgewave's KafkaConsumer is a synchronous, single-thread polling
consumer (`Kuestenlogik.Surgewave.Client/Consumer/KafkaConsumer.cs`) —
it does NOT implement the Java AsyncKafkaConsumer state machine that
each of these bugs targets:

- **KAFKA-20426** (busy-loop on group.id + assign()): Java's bug was
  in the AsyncConsumer's `MembershipManagerImpl` returning
  `maximumTimeToWait = 0` while in the UNSUBSCRIBED state, causing the
  event-loop to spin. Surgewave has no UNSUBSCRIBED state, no
  HeartbeatRequestManager, no async event-loop. `Assign()` and
  `Subscribe()` both just set the `_subscriptions` dictionary.

- **KAFKA-20428** (unsubscribe applied pending assignment updates):
  Java's bug was a race in the background-thread reconciliation queue.
  Surgewave has no background-thread reconciliation — every `Poll()`
  reads directly from the current subscriptions dictionary.

- **KAFKA-20332** (race: records for revoked partitions): Java's bug
  was the app-thread reading from `Fetcher` after revoke had updated
  the assignment but before the read was synchronized. Surgewave's
  consumer is single-threaded; no race window.

Surgewave's `KafkaConsumer` is intentionally simple. When/if a true
async/cooperative-rebalance variant is added (analog `AsyncKafkaConsumer`),
revisit these three Kafka commits.

Refs: 44bafc60e7, 4b9eddc132, b954b35d0a, 7e1c9db92f.

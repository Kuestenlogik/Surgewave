---
title: <fill in before the tag — matches the v0.5 milestone theme>
version: 0.5.0
---

<One-sentence frame for what 0.5 is about. Replace this placeholder
the moment the first 0.5 work lands.>

## Highlights

<!-- Add a section per landed feature as the work merges. Pattern:
### <headline> (#issue)
<2-4 sentences>
-->

## Fixes

### A multi-batch produce section is refused as `InvalidRecord`, not `CorruptMessage` (#125)

A produce request carries exactly one record batch per partition — Kafka enforces it at parse time
and its own producer cannot build anything else. Surgewave already refused such a section, but by
accident: it reached the append, whose CRC is computed over the whole concatenation and cannot
match the first batch's CRC field, so the answer was `CorruptMessage` (2). That blames transport
for a request the protocol does not permit, and it sent anyone debugging it looking for a network
fault that was not there.

The refusal is now explicit and carries Kafka's own code, `InvalidRecord` (87). A section too short
to hold a batch header, or one whose first batch overruns it, is refused the same way instead of
surfacing as a generic `Unknown`. Nothing about accepted traffic changes: the check is two integer
reads on the produce path, and for a conforming single-batch request the answer is immediate.

## Breaking changes

### `SnapshotNotFound` moves from error code 87 to 98 (wire-visible)

Surgewave's `SnapshotNotFound` was numbered 87, which is Kafka's `INVALID_RECORD`; the real
`SNAPSHOT_NOT_FOUND` is 98 and was unused. The Raft snapshot-fetch path put 87 on the wire, so a
Kafka-compatible client decoded "this record failed validation on the broker" from a response about
a missing snapshot.

`SnapshotNotFound` is now 98 and `InvalidRecord` occupies 87, matching
`org.apache.kafka.common.protocol.Errors`.

**Rolling upgrades:** brokers of mixed versions disagree about what 87 means on the Raft
fetch-snapshot path for the duration of the roll. The exchange is broker-to-broker and the code is
informational there — a follower that cannot get a snapshot retries either way — so the practical
effect is a misleading log line during the window, not a stall. Upgrading all brokers in one roll
avoids it entirely.

## Breaking changes

### Kafka produce now validates the producer CRC instead of overwriting it (#85)

The broker used to recompute every incoming batch's CRC-32C and overwrite the field, which
silently healed corrupt bytes into the log. Produce over the Kafka wire now validates the CRC the
producer sent and answers a mismatch with `CorruptMessage` (error code 2), the same as Kafka.

This costs nothing: it is the same single pass the append already made, plus a four-byte compare.
Real clients (librdkafka, the Java client, Confluent.Kafka) all write a correct CRC32C and are
unaffected. Hand-rolled clients that send a zero or stale CRC and relied on the broker fixing it
will now get `CorruptMessage`.

Note that a produce request must carry one record batch per partition, not several concatenated
ones — the broker has always assumed this when parsing the batch header, and validation now makes
it visible. Every mainstream client sends a single batch.

### `NativeCompressionCodec.CompressWithHeader` replaced by `TryCompressWithHeader` (#86)

The old method allocated up to three arrays per compressed frame and copied the
payload even when compression was rejected. It is replaced by

```csharp
bool TryCompressWithHeader(ReadOnlySpan<byte> data, out byte[]? pooledBuffer, out int frameLength)
```

which compresses into a single pooled buffer and rents nothing at all when the
payload is incompressible. Migration: on `true`, the frame is
`pooledBuffer[0..frameLength)` and **you own the rent** — return it to
`ArrayPool<byte>.Shared` once the bytes are on the wire (a `finally` around the
write). On `false`, send the original payload unchanged; `pooledBuffer` is null.
The wire format is byte-identical, so old and new peers interoperate.

## Acknowledgements

<!-- Optional. -->

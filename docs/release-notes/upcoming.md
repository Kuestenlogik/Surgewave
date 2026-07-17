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

<!-- Grouped notable fixes. -->

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

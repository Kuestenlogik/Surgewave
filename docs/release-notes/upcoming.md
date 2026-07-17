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

# ADR-012: Zero-Copy & High-Performance Patterns

## Status

Accepted

## Date

2026-04

## Context

Surgewave's core design goal is to match or exceed Kafka's throughput and latency characteristics while being competitive with Aeron for raw transport performance. This requires minimizing memory allocations on the hot path, avoiding unnecessary data copies between kernel and user space, and exploiting modern CPU hardware features (SIMD, branch prediction, cache-line alignment).

.NET provides the building blocks --- `Span<T>`, `ReadOnlySpan<T>`, `ref struct`, `ArrayPool<T>`, `MemoryMappedFile`, and `System.Runtime.Intrinsics` --- but they must be applied systematically across the protocol layer, storage layer, and message pipeline.

### Alternatives Considered

- **Standard stream-based I/O with BinaryReader/BinaryWriter:** Simple but allocates on every read/write (byte arrays, strings). Unacceptable for a system processing millions of messages per second.
- **Unsafe pointer arithmetic throughout:** Maximum performance but fragile, hard to maintain, and prone to memory safety bugs. Better to use safe `Span<T>` abstractions that compile down to the same machine code.
- **Third-party serialization frameworks (e.g., Google.Protobuf for wire protocol):** Adds allocation overhead and prevents fine-grained control over buffer layout. Surgewave's native protocol needs byte-level control.

## Decision

Adopt a layered zero-copy strategy using `ref struct` payload writers/readers, tiered buffer pooling, memory-mapped file segments, and SIMD-accelerated utilities.

### ref struct Payload Writers and Readers

`SurgewavePayloadWriter` and `SurgewavePayloadReader` are `ref struct` types that operate directly on `Span<byte>` and `ReadOnlySpan<byte>` buffers. Being `ref struct` guarantees they live on the stack --- no heap allocation, no GC pressure.

The writer provides typed methods (`WriteInt8`, `WriteInt16`, `WriteInt32`, `WriteInt64`, `WriteString`, `WriteBytes`, `WriteBoolean`, `WriteNullableString`) that encode values directly into the buffer using `BinaryPrimitives` for big-endian byte order. String encoding uses `Encoding.UTF8.GetBytes(ReadOnlySpan<char>, Span<byte>)` to write directly into the output buffer without intermediate allocations.

The reader mirrors the writer with `ReadInt8`, `ReadInt16`, `ReadInt32`, `ReadInt64`, `ReadString`, `ReadBytes`, `ReadBoolean`, `ReadNullableString`, plus `Skip` and `ReadRaw` for raw byte slicing. `ReadBytes` returns `ReadOnlySpan<byte>` pointing into the original buffer --- true zero-copy.

### BinaryPrimitives for Big-Endian Encoding

All integer encoding uses `System.Buffers.Binary.BinaryPrimitives` (`WriteInt32BigEndian`, `ReadInt64BigEndian`, etc.) rather than manual bit shifting. This compiles to optimal machine code on both little-endian and big-endian architectures and avoids the correctness bugs that manual endian conversion introduces.

### Tiered Buffer Pooling

`BufferPool` provides a tiered pooling strategy tuned for message processing workloads:

- **Small (4 KB)** --- protocol headers, small messages. Pool of 100 buffers via `ArrayPool<byte>`.
- **Medium (64 KB)** --- standard message batches. Pool of 50 buffers.
- **Large (1 MB)** --- large batches, bulk transfers. Pool of 20 buffers.
- **XLarge (16 MB)** --- maximum batch size scenarios. `ConcurrentBag`-based pool, capped at 8 buffers.

`RentedBuffer` is a `readonly struct` implementing `IDisposable` that automatically returns the buffer to the pool. Implicit conversions to `byte[]`, `Span<byte>`, and `Memory<byte>` minimize ceremony at call sites.

At the storage layer, `DefaultSurgewaveBufferPool` and `PooledSurgewaveBuffer` provide the same pattern via the `ISurgewaveBuffer` / `ISurgewaveBufferPool` abstractions, used by storage engines and the write pipeline.

### Memory-Mapped File Segments

`FileMmapBuffer` wraps a `MemoryMappedViewAccessor` and exposes the mapped region as `ReadOnlySpan<byte>` via unsafe pointer access (`AcquirePointer` / `ReleasePointer`). This provides true zero-copy reads from disk --- the OS kernel maps file pages directly into user-space virtual memory, bypassing the read syscall and kernel-to-user copy.

`FileMmapManager` handles the lifecycle of memory-mapped files, lazily extending the mapping as the file grows. Slicing (`FileMmapBuffer.Slice`) creates a new buffer sharing the same accessor but acquiring its own pointer reference for safe independent disposal.

### SIMD Optimizations

Five SIMD utility classes in `Kuestenlogik.Surgewave.Core.Util` accelerate hot-path operations:

- **`SimdSpanComparer`** --- AVX2/SSE2 byte-span equality comparison and pattern search. Processes 32 bytes per cycle on AVX2, 16 bytes on SSE2, with scalar fallback. Used for header key matching in the message pipeline.
- **`SimdBufferCopy`** --- AVX2/SSE2 bulk memory copy. Processes 256 bytes at a time (8 x 32-byte vectors) for cache-line efficiency. Includes `Fill` and `Zero` operations. Threshold at 256 bytes below which `Span.CopyTo` is faster.
- **`SimdVarIntScanner`** --- AVX2/SSE2 VarInt boundary detection using `MoveMask` to find terminator bytes (bit 7 = 0) across 16/32 bytes simultaneously. Enables fast record batch parsing by locating VarInt boundaries without decoding. Includes `BatchReadVarInts`, `ScanRecordOffsets`, and `ExtractKeyValue` for zero-copy record field extraction.
- **`SimdBigEndian`** --- SSSE3 `PSHUFB` / AVX2 `VPSHUFB` byte-swap operations for batch big-endian encoding/decoding. Processes 4 Int64s or 8 Int32s per AVX2 cycle using pre-computed shuffle masks. Specialized two- and four-element variants (`Write2Int64sBigEndian`, `Write4Int32sBigEndian`) for common patterns like offset+timestamp pairs.
- **`SimdByteArrayComparer`** --- SIMD-accelerated byte array equality for use in dictionary lookups and hash comparisons.

All SIMD utilities use `[MethodImpl(MethodImplOptions.AggressiveInlining)]` and provide transparent scalar fallbacks, so the same code runs correctly on hardware without AVX2/SSE2 support.

### CRC32C Hardware Acceleration

`Crc32C` uses hardware CRC32 instructions when available for record batch integrity verification, matching Kafka's CRC32C checksum format.

## Consequences

- **Zero-allocation protocol path** --- producing and consuming messages through the native protocol involves no heap allocations for serialization/deserialization. Verified via `[MemoryDiagnoser]` benchmarks.
- **P99 latency reduction** --- eliminating GC pauses on the hot path keeps tail latencies predictable. The `ref struct` writer/reader pattern prevents accidental boxing.
- **Memory-mapped reads** bypass the kernel copy, reducing per-message CPU cost for consumers reading recent data that is likely in the page cache.
- **SIMD utilities** provide measurable speedups for batch operations (VarInt scanning, buffer comparison, endian conversion) but add platform-specific code paths that must be tested on both x86-64 and ARM64.
- **Increased code complexity** --- `ref struct` types cannot be used in async methods, stored in fields, or boxed. This constrains API design and requires callers to process payloads synchronously within a stack frame. The trade-off is acceptable because protocol encoding/decoding is inherently synchronous and CPU-bound.
- **Buffer pool sizing** is tuned for Kafka-typical workloads. Deployments with unusual message size distributions may need to adjust pool parameters via configuration.

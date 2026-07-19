# ADR 015 — Remove the dead `SimdVarIntScanner` and `SimdBufferCopy` helpers

Status: Accepted
Date: 2026-07-19
Related: #85, [ADR 012 — Zero-copy & high performance](012-zero-copy-high-performance.md)

## Context

Issue #85 ("CRC32C is a latency-bound scalar chain; batches hashed twice per append; hand-rolled SIMD
layer is dead code") named a "hand-rolled SIMD layer" as dead code. Auditing the codebase resolved what
that layer actually is:

- There is **no** hand-rolled SIMD/PCLMULQDQ CRC implementation. `Core/Util/Crc32C.cs` is the single CRC
  path and it is live (SSE4.2 `Crc32` / ARM `Crc32C` intrinsics with a software table fallback).
- Two SIMD helpers in `Core/Util` are genuinely dead — zero production (`src/`) callers, confirmed by grep:
  - **`SimdVarIntScanner`** (AVX2/SSE2 varint boundary scan via `MoveMask`). The record-batch parser
    (`RecordBatchSerializer.ParseRecordBatch`) decodes varints with `VarintCodec`, never this scanner.
    Its only references were its own tests and a benchmark. Its non-terminator-scan methods were scalar
    anyway (`BatchReadVarInts` loops the scalar `ReadVarIntFast`).
  - **`SimdBufferCopy`** (AVX2/SSE2 bulk copy/fill/zero). Zero callers anywhere — not even a test or
    benchmark. Bulk copies in the hot path use `Span.CopyTo` / pooled buffers.

Keeping unwired "SIMD record-batch" code is misleading: it implies an acceleration path that does not
exist and invites the assumption that #85's CRC work belongs there.

## Decision

Delete `SimdVarIntScanner.cs`, `SimdBufferCopy.cs`, and their orphaned test (`SimdVarIntScannerTests.cs`)
and benchmark (`VarIntScannerBenchmarks.cs`). Update the benchmark catalogue and ADR 012's SIMD inventory
accordingly.

CRC32C acceleration is pursued where it lives — in `Crc32C.cs`, via the hardware CRC32 instructions and a
3-way interleaved + carry-less-multiply (PCLMULQDQ/PMULL) fold for large buffers (tracked under #85) —
not by wiring in a varint scanner.

The three remaining SIMD helpers are live and retained: `SimdSpanComparer` (header key matching),
`SimdBigEndian` (batch big-endian encode), `SimdByteArrayComparer` (compaction key equality).

## Consequences

- One CRC path, no misleading dead "SIMD record-batch" layer; smaller surface to maintain and reason about.
- No behavioural change — the deleted code had no production callers, so the compiler proves nothing breaks.
- If a SIMD varint scan is ever wanted, it should be reintroduced as its own benchmark-gated effort wired
  directly into `ParseRecordBatch`, tracked by a fresh issue — not carried as unreferenced code.

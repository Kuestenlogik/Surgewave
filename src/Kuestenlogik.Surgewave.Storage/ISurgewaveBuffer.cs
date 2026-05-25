using System.Buffers;

namespace Kuestenlogik.Surgewave.Storage;

/// <summary>
/// Zero-copy buffer abstraction for Surgewave storage.
/// Represents a memory region with explicit lifetime/ownership semantics.
///
/// Implementations:
/// - MemorySurgewaveBuffer: Wraps pooled byte arrays
/// - MmapSurgewaveBuffer: Wraps memory-mapped file regions
/// - ArrowSurgewaveBuffer: Wraps Apache Arrow buffers (in separate package)
/// </summary>
public interface ISurgewaveBuffer : IDisposable
{
    /// <summary>
    /// Length of the buffer in bytes.
    /// </summary>
    int Length { get; }

    /// <summary>
    /// Whether this buffer is empty (Length == 0).
    /// </summary>
    bool IsEmpty => Length == 0;

    /// <summary>
    /// Get a read-only span over the buffer contents.
    /// Only valid while the buffer is not disposed.
    /// WARNING: Do not store or use across await boundaries.
    /// </summary>
    ReadOnlySpan<byte> Span { get; }

    /// <summary>
    /// Get a read-only memory over the buffer contents.
    /// Safe to use across await boundaries, but buffer must not be disposed.
    /// </summary>
    ReadOnlyMemory<byte> Memory { get; }

    /// <summary>
    /// Try to get the underlying memory for scenarios where it's available.
    /// Returns false for pinned/native buffers that don't have managed backing.
    /// </summary>
    bool TryGetMemory(out ReadOnlyMemory<byte> memory);

    /// <summary>
    /// Pin the buffer and get a memory handle for native I/O operations.
    /// The handle must be disposed after use.
    /// </summary>
    MemoryHandle Pin();

    /// <summary>
    /// Copy buffer contents to a destination span.
    /// </summary>
    void CopyTo(Span<byte> destination);

    /// <summary>
    /// Copy buffer contents to a byte array.
    /// Use only when a copy is actually needed.
    /// </summary>
    byte[] ToArray();

    /// <summary>
    /// Create a slice of this buffer.
    /// The returned buffer shares the same underlying storage.
    /// </summary>
    ISurgewaveBuffer Slice(int start, int length);

    /// <summary>
    /// Create a slice of this buffer from start to end.
    /// </summary>
    ISurgewaveBuffer Slice(int start) => Slice(start, Length - start);
}

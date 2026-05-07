using System.Buffers;

namespace Kuestenlogik.Surgewave.Core.Pipeline;

/// <summary>
/// A pooled buffer that wraps IMemoryOwner for zero-copy operations.
/// Must be disposed to return the buffer to the pool.
/// </summary>
public readonly struct PooledBuffer : IDisposable
{
    private readonly IMemoryOwner<byte>? _owner;
    private readonly int _length;

    public Memory<byte> Memory => _owner?.Memory.Slice(0, _length) ?? Memory<byte>.Empty;
    public Span<byte> Span => Memory.Span;
    public int Length => _length;
    public bool IsEmpty => _length == 0;

    private PooledBuffer(IMemoryOwner<byte> owner, int length)
    {
        _owner = owner;
        _length = length;
    }

    /// <summary>
    /// Rent a buffer from the shared pool.
    /// </summary>
    public static PooledBuffer Rent(int minimumLength)
    {
        if (minimumLength <= 0)
            return default;

        var owner = MemoryPool<byte>.Shared.Rent(minimumLength);
        return new PooledBuffer(owner, minimumLength);
    }

    /// <summary>
    /// Rent a buffer and copy data into it.
    /// </summary>
    public static PooledBuffer RentAndCopy(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            return default;

        var owner = MemoryPool<byte>.Shared.Rent(data.Length);
        data.CopyTo(owner.Memory.Span);
        return new PooledBuffer(owner, data.Length);
    }

    /// <summary>
    /// Return buffer to pool.
    /// </summary>
    public void Dispose()
    {
        _owner?.Dispose();
    }
}

/// <summary>
/// A read-only pooled buffer for fetch responses.
/// </summary>
public readonly struct PooledReadOnlyBuffer : IDisposable
{
    private readonly IMemoryOwner<byte>? _owner;
    private readonly int _length;

    public ReadOnlyMemory<byte> Memory => _owner?.Memory.Slice(0, _length) ?? ReadOnlyMemory<byte>.Empty;
    public ReadOnlySpan<byte> Span => Memory.Span;
    public int Length => _length;
    public bool IsEmpty => _length == 0;

    internal PooledReadOnlyBuffer(IMemoryOwner<byte> owner, int length)
    {
        _owner = owner;
        _length = length;
    }

    /// <summary>
    /// Rent a buffer and copy data into it.
    /// </summary>
    public static PooledReadOnlyBuffer RentAndCopy(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            return default;

        var owner = MemoryPool<byte>.Shared.Rent(data.Length);
        data.CopyTo(owner.Memory.Span);
        return new PooledReadOnlyBuffer(owner, data.Length);
    }

    public void Dispose()
    {
        _owner?.Dispose();
    }
}

/// <summary>
/// Extension methods for working with pooled buffers.
/// </summary>
public static class PooledBufferExtensions
{
    /// <summary>
    /// Write data to a pooled buffer.
    /// </summary>
    public static PooledBuffer ToPooledBuffer(this ReadOnlySpan<byte> data)
    {
        return PooledBuffer.RentAndCopy(data);
    }

    /// <summary>
    /// Write data to a pooled buffer.
    /// </summary>
    public static PooledBuffer ToPooledBuffer(this byte[] data)
    {
        return PooledBuffer.RentAndCopy(data);
    }
}

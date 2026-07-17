using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Minimal ArrayPool-backed <see cref="IBufferWriter{T}"/> used by pooled decompression.
/// <see cref="DetachBuffer"/> transfers ownership of the rented array to the caller (the
/// DecompressPooled contract: the caller returns it to <see cref="ArrayPool{T}.Shared"/>);
/// <see cref="Dispose"/> returns the rent only when it was never detached, so a corrupt frame
/// gives the buffer back instead of leaking it and a detached buffer is never returned twice.
/// </summary>
internal sealed class PooledArrayBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[]? _buffer;
    private int _written;

    public PooledArrayBufferWriter(int initialCapacity)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(initialCapacity, 256));
    }

    public void Advance(int count)
    {
        ObjectDisposedException.ThrowIf(_buffer is null, this);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, _buffer.Length - _written);
        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    /// <summary>
    /// Hands the rented array to the caller, who becomes responsible for returning it.
    /// The writer is unusable afterwards.
    /// </summary>
    public (byte[] Buffer, int Length) DetachBuffer()
    {
        ObjectDisposedException.ThrowIf(_buffer is null, this);
        var buffer = _buffer;
        _buffer = null;
        return (buffer, _written);
    }

    public void Dispose()
    {
        var buffer = _buffer;
        _buffer = null;
        if (buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    [MemberNotNull(nameof(_buffer))]
    private void EnsureCapacity(int sizeHint)
    {
        ObjectDisposedException.ThrowIf(_buffer is null, this);
        if (sizeHint < 1)
        {
            sizeHint = 1;
        }

        if (_buffer.Length - _written >= sizeHint)
        {
            return;
        }

        var grown = ArrayPool<byte>.Shared.Rent(Math.Max(_buffer.Length * 2, _written + sizeHint));
        _buffer.AsSpan(0, _written).CopyTo(grown);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = grown;
    }
}

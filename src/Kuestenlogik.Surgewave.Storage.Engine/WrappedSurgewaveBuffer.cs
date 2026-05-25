using System.Buffers;

namespace Kuestenlogik.Surgewave.Storage.Engine;

/// <summary>
/// A Surgewave buffer that wraps existing memory without owning it.
/// </summary>
public sealed class WrappedSurgewaveBuffer : ISurgewaveBuffer
{
    private ReadOnlyMemory<byte> _memory;
    private bool _disposed;

    public int Length => _memory.Length;

    public ReadOnlySpan<byte> Span
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _memory.Span;
        }
    }

    public ReadOnlyMemory<byte> Memory
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _memory;
        }
    }

    public WrappedSurgewaveBuffer(ReadOnlyMemory<byte> memory)
    {
        _memory = memory;
    }

    public bool TryGetMemory(out ReadOnlyMemory<byte> memory)
    {
        if (_disposed)
        {
            memory = default;
            return false;
        }
        memory = _memory;
        return true;
    }

    public MemoryHandle Pin()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _memory.Pin();
    }

    public void CopyTo(Span<byte> destination)
    {
        Span.CopyTo(destination);
    }

    public byte[] ToArray()
    {
        return Span.ToArray();
    }

    public ISurgewaveBuffer Slice(int start, int length)
    {
        if (start < 0 || length < 0 || start + length > _memory.Length)
            throw new ArgumentOutOfRangeException(nameof(start), "Slice parameters out of range");

        ObjectDisposedException.ThrowIf(_disposed, this);

        return new WrappedSurgewaveBuffer(_memory.Slice(start, length));
    }

    public void Dispose()
    {
        _disposed = true;
        _memory = default;
    }
}

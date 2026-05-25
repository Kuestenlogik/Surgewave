using System.Buffers;

namespace Kuestenlogik.Surgewave.Storage.Engine;

/// <summary>
/// A Surgewave buffer backed by ArrayPool for efficient memory reuse.
/// </summary>
public sealed class PooledSurgewaveBuffer : ISurgewaveWritableBuffer
{
    private byte[]? _array;
    private readonly int _length;
    private readonly int _offset;
    private readonly bool _ownsArray;

    public int Length => _length;

    public ReadOnlySpan<byte> Span
    {
        get
        {
            ObjectDisposedException.ThrowIf(_array == null, this);
            return _array.AsSpan(_offset, _length);
        }
    }

    Span<byte> ISurgewaveWritableBuffer.Span
    {
        get
        {
            ObjectDisposedException.ThrowIf(_array == null, this);
            return _array.AsSpan(_offset, _length);
        }
    }

    public ReadOnlyMemory<byte> Memory
    {
        get
        {
            ObjectDisposedException.ThrowIf(_array == null, this);
            return _array.AsMemory(_offset, _length);
        }
    }

    Memory<byte> ISurgewaveWritableBuffer.Memory
    {
        get
        {
            ObjectDisposedException.ThrowIf(_array == null, this);
            return _array.AsMemory(_offset, _length);
        }
    }

    /// <summary>
    /// Create a new pooled buffer of the specified size.
    /// </summary>
    public PooledSurgewaveBuffer(int length)
    {
        _array = ArrayPool<byte>.Shared.Rent(length);
        _length = length;
        _offset = 0;
        _ownsArray = true;
    }

    /// <summary>
    /// Create a buffer wrapping an existing array (for slicing).
    /// </summary>
    private PooledSurgewaveBuffer(byte[] array, int offset, int length, bool ownsArray)
    {
        _array = array;
        _offset = offset;
        _length = length;
        _ownsArray = ownsArray;
    }

    public bool TryGetMemory(out ReadOnlyMemory<byte> memory)
    {
        if (_array == null)
        {
            memory = default;
            return false;
        }
        memory = _array.AsMemory(_offset, _length);
        return true;
    }

    public MemoryHandle Pin()
    {
        ObjectDisposedException.ThrowIf(_array == null, this);
        return _array.AsMemory(_offset, _length).Pin();
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
        if (start < 0 || length < 0 || start + length > _length)
            throw new ArgumentOutOfRangeException(nameof(start), "Slice parameters out of range");

        ObjectDisposedException.ThrowIf(_array == null, this);

        // Slice doesn't own the array - parent is responsible for return
        return new PooledSurgewaveBuffer(_array, _offset + start, length, ownsArray: false);
    }

    public void Dispose()
    {
        var array = _array;
        _array = null;

        if (array != null && _ownsArray)
        {
            ArrayPool<byte>.Shared.Return(array);
        }
    }
}

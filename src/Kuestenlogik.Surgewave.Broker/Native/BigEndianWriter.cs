using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Broker.Native;

/// <summary>
/// A binary writer that writes multi-byte values in big-endian (network) byte order.
/// Optimized to use ArrayPool instead of MemoryStream to reduce allocations.
/// Supports object pooling for high-frequency use cases like fetch responses.
/// Implements IPayloadWriter for shared payload serialization.
/// </summary>
internal sealed class BigEndianWriter : IDisposable, IPayloadWriter
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<BigEndianWriter> s_pool = new();
    private static int s_poolSize;
    private const int MaxPoolSize = 64;

    private byte[] _buffer;
    private int _position;
    private bool _disposed;
    private bool _fromPool;

    public BigEndianWriter(int initialCapacity = 256)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
        _position = 0;
    }

    /// <summary>
    /// Rent a pooled writer for high-frequency operations.
    /// Must be returned via ReturnToPool() or Dispose().
    /// </summary>
    public static BigEndianWriter Rent(int initialCapacity = 1024)
    {
        if (s_pool.TryTake(out var writer))
        {
            Interlocked.Decrement(ref s_poolSize);
            writer._disposed = false;
            writer._fromPool = true;
            // Ensure capacity
            if (writer._buffer.Length < initialCapacity)
            {
                ArrayPool<byte>.Shared.Return(writer._buffer);
                writer._buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
            }
            return writer;
        }

        var newWriter = new BigEndianWriter(initialCapacity);
        newWriter._fromPool = true;
        return newWriter;
    }

    /// <summary>
    /// Return writer to pool for reuse. Resets position to 0.
    /// </summary>
    public void ReturnToPool()
    {
        if (!_fromPool || _disposed) return;

        _position = 0;

        if (Interlocked.Increment(ref s_poolSize) <= MaxPoolSize)
        {
            s_pool.Add(this);
        }
        else
        {
            Interlocked.Decrement(ref s_poolSize);
            DisposeInternal();
        }
    }

    public int Length => _position;

    private void EnsureCapacity(int additionalBytes)
    {
        var required = _position + additionalBytes;
        if (required <= _buffer.Length) return;

        // Grow buffer
        var newSize = Math.Max(_buffer.Length * 2, required);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        Buffer.BlockCopy(_buffer, 0, newBuffer, 0, _position);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = newBuffer;
    }

    public void Write(byte value)
    {
        EnsureCapacity(1);
        _buffer[_position++] = value;
    }

    public void Write(short value)
    {
        EnsureCapacity(2);
        BinaryPrimitives.WriteInt16BigEndian(_buffer.AsSpan(_position, 2), value);
        _position += 2;
    }

    public void Write(ushort value)
    {
        EnsureCapacity(2);
        BinaryPrimitives.WriteUInt16BigEndian(_buffer.AsSpan(_position, 2), value);
        _position += 2;
    }

    public void Write(int value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(_position, 4), value);
        _position += 4;
    }

    /// <summary>
    /// Patch an int32 value at a specific position (for writing placeholders then fixing later).
    /// </summary>
    public void PatchInt32(int position, int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(position, 4), value);
    }

    public void Write(uint value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteUInt32BigEndian(_buffer.AsSpan(_position, 4), value);
        _position += 4;
    }

    public void Write(long value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteInt64BigEndian(_buffer.AsSpan(_position, 8), value);
        _position += 8;
    }

    public void Write(ulong value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteUInt64BigEndian(_buffer.AsSpan(_position, 8), value);
        _position += 8;
    }

    public void Write(byte[] value)
    {
        EnsureCapacity(value.Length);
        Buffer.BlockCopy(value, 0, _buffer, _position, value.Length);
        _position += value.Length;
    }

    public void Write(ReadOnlySpan<byte> value)
    {
        EnsureCapacity(value.Length);
        value.CopyTo(_buffer.AsSpan(_position));
        _position += value.Length;
    }

    /// <summary>
    /// Writes a string as a 2-byte big-endian length prefix followed by UTF-8 bytes.
    /// Uses stackalloc for small strings to avoid allocations.
    /// </summary>
    public void WriteString(string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        Write((short)byteCount);

        EnsureCapacity(byteCount);
        Encoding.UTF8.GetBytes(value.AsSpan(), _buffer.AsSpan(_position, byteCount));
        _position += byteCount;
    }

    /// <summary>
    /// Writes a nullable string. If null, writes -1 as length. Otherwise same as WriteString.
    /// </summary>
    public void WriteNullableString(string? value)
    {
        if (value == null)
        {
            Write((short)-1);
            return;
        }
        WriteString(value);
    }

    /// <summary>
    /// Returns a copy of the written data. Caller owns the returned array.
    /// </summary>
    public byte[] ToArray()
    {
        var result = new byte[_position];
        Buffer.BlockCopy(_buffer, 0, result, 0, _position);
        return result;
    }

    /// <summary>
    /// Returns the written data as a span. Valid only until next write or dispose.
    /// </summary>
    public ReadOnlySpan<byte> AsSpan() => _buffer.AsSpan(0, _position);

    /// <summary>
    /// Returns the written data as memory. Valid only until next write or dispose.
    /// </summary>
    public ReadOnlyMemory<byte> AsMemory() => _buffer.AsMemory(0, _position);

    // === SIMD Batch Write Methods ===

    /// <summary>
    /// Write multiple Int64 values in big-endian format using SIMD when available.
    /// More efficient than calling Write(long) multiple times.
    /// </summary>
    public void WriteBatch(ReadOnlySpan<long> values)
    {
        var byteCount = values.Length * 8;
        EnsureCapacity(byteCount);
        SimdBigEndian.WriteInt64sBigEndian(_buffer.AsSpan(_position, byteCount), values);
        _position += byteCount;
    }

    /// <summary>
    /// Write multiple Int32 values in big-endian format using SIMD when available.
    /// </summary>
    public void WriteBatch(ReadOnlySpan<int> values)
    {
        var byteCount = values.Length * 4;
        EnsureCapacity(byteCount);
        SimdBigEndian.WriteInt32sBigEndian(_buffer.AsSpan(_position, byteCount), values);
        _position += byteCount;
    }

    /// <summary>
    /// Write multiple Int16 values in big-endian format using SIMD when available.
    /// </summary>
    public void WriteBatch(ReadOnlySpan<short> values)
    {
        var byteCount = values.Length * 2;
        EnsureCapacity(byteCount);
        SimdBigEndian.WriteInt16sBigEndian(_buffer.AsSpan(_position, byteCount), values);
        _position += byteCount;
    }

    /// <summary>
    /// Write 2 consecutive Int64 values in big-endian format using SIMD.
    /// Common pattern for offset + timestamp pairs.
    /// </summary>
    public void Write2Long(long value1, long value2)
    {
        EnsureCapacity(16);
        SimdBigEndian.Write2Int64sBigEndian(_buffer.AsSpan(_position, 16), value1, value2);
        _position += 16;
    }

    /// <summary>
    /// Write 4 consecutive Int32 values in big-endian format using SIMD.
    /// </summary>
    public void Write4Int(int value1, int value2, int value3, int value4)
    {
        EnsureCapacity(16);
        SimdBigEndian.Write4Int32sBigEndian(_buffer.AsSpan(_position, 16), value1, value2, value3, value4);
        _position += 16;
    }

    // === IPayloadWriter interface implementation ===

    void IPayloadWriter.WriteInt8(sbyte value) => Write((byte)value);
    void IPayloadWriter.WriteUInt8(byte value) => Write(value);
    void IPayloadWriter.WriteInt16(short value) => Write(value);
    void IPayloadWriter.WriteUInt16(ushort value) => Write(value);
    void IPayloadWriter.WriteInt32(int value) => Write(value);
    void IPayloadWriter.WriteUInt32(uint value) => Write(value);
    void IPayloadWriter.WriteInt64(long value) => Write(value);
    void IPayloadWriter.WriteUInt64(ulong value) => Write(value);
    void IPayloadWriter.WriteString(string? value) => WriteNullableString(value);
    void IPayloadWriter.WriteBytes(ReadOnlySpan<byte> value) => Write(value);

    public void Dispose()
    {
        if (_disposed) return;

        if (_fromPool)
        {
            ReturnToPool();
        }
        else
        {
            DisposeInternal();
        }
    }

    private void DisposeInternal()
    {
        if (_disposed) return;
        _disposed = true;
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = null!;
    }
}

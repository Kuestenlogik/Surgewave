using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Utilities for reading and writing Kafka protocol primitives
/// All multi-byte values are big-endian (network byte order)
/// </summary>
public static class KafkaProtocolPrimitives
{
    /// <summary>
    /// Encode an unsigned integer as a varint (variable-length integer)
    /// Uses LZCNT-based length calculation for optimal performance.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteVarInt(Span<byte> buffer, int value)
    {
        uint uvalue = (uint)value;

        // Fast path: single byte (values 0-127, very common)
        if (uvalue < 0x80)
        {
            buffer[0] = (byte)uvalue;
            return 1;
        }

        // Fast path: two bytes (values 128-16383)
        if (uvalue < 0x4000)
        {
            buffer[0] = (byte)((uvalue & 0x7F) | 0x80);
            buffer[1] = (byte)(uvalue >> 7);
            return 2;
        }

        // Fast path: three bytes (values 16384-2097151)
        if (uvalue < 0x200000)
        {
            buffer[0] = (byte)((uvalue & 0x7F) | 0x80);
            buffer[1] = (byte)(((uvalue >> 7) & 0x7F) | 0x80);
            buffer[2] = (byte)(uvalue >> 14);
            return 3;
        }

        // General path for larger values (4-5 bytes)
        return WriteVarIntSlow(buffer, uvalue);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int WriteVarIntSlow(Span<byte> buffer, uint uvalue)
    {
        int bytesWritten = 0;
        while ((uvalue & ~0x7FU) != 0)
        {
            buffer[bytesWritten++] = (byte)((uvalue & 0x7F) | 0x80);
            uvalue >>= 7;
        }
        buffer[bytesWritten++] = (byte)uvalue;
        return bytesWritten;
    }

    /// <summary>
    /// Decode a varint to an integer using optimized branch-free decoding for small values.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (int value, int bytesRead) ReadVarInt(ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty)
            throw new InvalidDataException("Incomplete VarInt");

        // Fast path: single byte (values 0-127, very common)
        byte b0 = buffer[0];
        if ((b0 & 0x80) == 0)
        {
            return (b0, 1);
        }

        if (buffer.Length < 2)
            throw new InvalidDataException("Incomplete VarInt");

        // Fast path: two bytes
        byte b1 = buffer[1];
        if ((b1 & 0x80) == 0)
        {
            return ((b0 & 0x7F) | (b1 << 7), 2);
        }

        if (buffer.Length < 3)
            throw new InvalidDataException("Incomplete VarInt");

        // Fast path: three bytes
        byte b2 = buffer[2];
        if ((b2 & 0x80) == 0)
        {
            return ((b0 & 0x7F) | ((b1 & 0x7F) << 7) | (b2 << 14), 3);
        }

        // Slower path for 4-5 byte values
        return ReadVarIntSlow(buffer);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (int value, int bytesRead) ReadVarIntSlow(ReadOnlySpan<byte> buffer)
    {
        int value = (buffer[0] & 0x7F) | ((buffer[1] & 0x7F) << 7) | ((buffer[2] & 0x7F) << 14);
        int bytesRead = 3;

        if (buffer.Length < 4)
            throw new InvalidDataException("Incomplete VarInt");

        byte b3 = buffer[3];
        value |= (b3 & 0x7F) << 21;
        bytesRead = 4;

        if ((b3 & 0x80) == 0)
            return (value, bytesRead);

        if (buffer.Length < 5)
            throw new InvalidDataException("Incomplete VarInt");

        byte b4 = buffer[4];
        if ((b4 & 0x80) != 0)
            throw new InvalidDataException("VarInt too long");

        value |= b4 << 28;
        return (value, 5);
    }

    /// <summary>
    /// Encode a long as a varlong using optimized paths for common sizes.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteVarLong(Span<byte> buffer, long value)
    {
        ulong uvalue = (ulong)value;

        // Fast path: single byte (values 0-127)
        if (uvalue < 0x80)
        {
            buffer[0] = (byte)uvalue;
            return 1;
        }

        // Fast path: two bytes (values 128-16383)
        if (uvalue < 0x4000)
        {
            buffer[0] = (byte)((uvalue & 0x7F) | 0x80);
            buffer[1] = (byte)(uvalue >> 7);
            return 2;
        }

        // Fast path: three bytes
        if (uvalue < 0x200000)
        {
            buffer[0] = (byte)((uvalue & 0x7F) | 0x80);
            buffer[1] = (byte)(((uvalue >> 7) & 0x7F) | 0x80);
            buffer[2] = (byte)(uvalue >> 14);
            return 3;
        }

        // General path for larger values
        return WriteVarLongSlow(buffer, uvalue);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int WriteVarLongSlow(Span<byte> buffer, ulong uvalue)
    {
        int bytesWritten = 0;
        while ((uvalue & ~0x7FUL) != 0)
        {
            buffer[bytesWritten++] = (byte)((uvalue & 0x7F) | 0x80);
            uvalue >>= 7;
        }
        buffer[bytesWritten++] = (byte)uvalue;
        return bytesWritten;
    }

    /// <summary>
    /// Decode a varlong to a long using optimized paths for common sizes.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long value, int bytesRead) ReadVarLong(ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty)
            throw new InvalidDataException("Incomplete VarLong");

        // Fast path: single byte
        byte b0 = buffer[0];
        if ((b0 & 0x80) == 0)
        {
            return (b0, 1);
        }

        if (buffer.Length < 2)
            throw new InvalidDataException("Incomplete VarLong");

        // Fast path: two bytes
        byte b1 = buffer[1];
        if ((b1 & 0x80) == 0)
        {
            return ((long)(b0 & 0x7F) | ((long)b1 << 7), 2);
        }

        if (buffer.Length < 3)
            throw new InvalidDataException("Incomplete VarLong");

        // Fast path: three bytes
        byte b2 = buffer[2];
        if ((b2 & 0x80) == 0)
        {
            return ((long)(b0 & 0x7F) | ((long)(b1 & 0x7F) << 7) | ((long)b2 << 14), 3);
        }

        // Slower path for larger values
        return ReadVarLongSlow(buffer);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (long value, int bytesRead) ReadVarLongSlow(ReadOnlySpan<byte> buffer)
    {
        long value = 0;
        int shift = 0;
        int bytesRead = 0;

        while (bytesRead < buffer.Length)
        {
            byte b = buffer[bytesRead++];
            value |= (long)(b & 0x7F) << shift;

            if ((b & 0x80) == 0)
            {
                return (value, bytesRead);
            }

            shift += 7;

            if (shift > 63)
            {
                throw new InvalidDataException("VarLong too long");
            }
        }

        throw new InvalidDataException("Incomplete VarLong");
    }

    /// <summary>
    /// Zigzag encoding for signed integers (used in Kafka protocol)
    /// </summary>
    public static uint ZigzagEncode(int value)
    {
        return (uint)((value << 1) ^ (value >> 31));
    }

    /// <summary>
    /// Zigzag decoding for signed integers
    /// </summary>
    public static int ZigzagDecode(uint value)
    {
        return (int)((value >> 1) ^ (-(value & 1)));
    }

    /// <summary>
    /// Zigzag encoding for signed longs
    /// </summary>
    public static ulong ZigzagEncode(long value)
    {
        return (ulong)((value << 1) ^ (value >> 63));
    }

    /// <summary>
    /// Zigzag decoding for signed longs
    /// </summary>
    public static long ZigzagDecode(ulong value)
    {
        return (long)(value >> 1) ^ (-(long)(value & 1));
    }
}

/// <summary>
/// Writer for Kafka protocol messages with proper encoding.
/// Uses ArrayBufferWriter for efficient buffer management without allocations.
/// </summary>
public sealed class KafkaProtocolWriter : IDisposable
{
    private readonly ArrayBufferWriter<byte> _buffer;
    private bool _disposed;

    public KafkaProtocolWriter(int initialCapacity = 256)
    {
        _buffer = new ArrayBufferWriter<byte>(initialCapacity);
    }

    public int Position => _buffer.WrittenCount;

    /// <summary>
    /// Resets the writer for reuse without releasing the internal buffer.
    /// </summary>
    public void Reset() => _buffer.ResetWrittenCount();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt8(sbyte value)
    {
        var span = _buffer.GetSpan(1);
        span[0] = (byte)value;
        _buffer.Advance(1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt16(short value)
    {
        var span = _buffer.GetSpan(2);
        BinaryPrimitives.WriteInt16BigEndian(span, value);
        _buffer.Advance(2);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt32(int value)
    {
        var span = _buffer.GetSpan(4);
        BinaryPrimitives.WriteInt32BigEndian(span, value);
        _buffer.Advance(4);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt64(long value)
    {
        var span = _buffer.GetSpan(8);
        BinaryPrimitives.WriteInt64BigEndian(span, value);
        _buffer.Advance(8);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteFloat64(double value)
    {
        var span = _buffer.GetSpan(8);
        BinaryPrimitives.WriteInt64BigEndian(span, BitConverter.DoubleToInt64Bits(value));
        _buffer.Advance(8);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUInt32(uint value)
    {
        var span = _buffer.GetSpan(4);
        BinaryPrimitives.WriteUInt32BigEndian(span, value);
        _buffer.Advance(4);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteBoolean(bool value)
    {
        var span = _buffer.GetSpan(1);
        span[0] = value ? (byte)1 : (byte)0;
        _buffer.Advance(1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteVarInt(int value)
    {
        var span = _buffer.GetSpan(5); // Max varint size
        var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(span, value);
        _buffer.Advance(bytesWritten);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteVarLong(long value)
    {
        var span = _buffer.GetSpan(10); // Max varlong size
        var bytesWritten = KafkaProtocolPrimitives.WriteVarLong(span, value);
        _buffer.Advance(bytesWritten);
    }

    public void WriteString(string? value)
    {
        if (value == null)
        {
            WriteInt16(-1);
            return;
        }

        // For small strings, use stackalloc to avoid allocation
        if (value.Length <= 128)
        {
            Span<byte> stackBuffer = stackalloc byte[value.Length * 3]; // Max UTF8 expansion
            var byteCount = Encoding.UTF8.GetBytes(value.AsSpan(), stackBuffer);
            WriteInt16((short)byteCount);
            var span = _buffer.GetSpan(byteCount);
            stackBuffer.Slice(0, byteCount).CopyTo(span);
            _buffer.Advance(byteCount);
        }
        else
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            WriteInt16((short)byteCount);
            var span = _buffer.GetSpan(byteCount);
            Encoding.UTF8.GetBytes(value.AsSpan(), span);
            _buffer.Advance(byteCount);
        }
    }

    public void WriteCompactString(string? value)
    {
        if (value == null)
        {
            WriteVarInt(0);
            return;
        }

        // For small strings, use stackalloc to avoid allocation
        if (value.Length <= 128)
        {
            Span<byte> stackBuffer = stackalloc byte[value.Length * 3]; // Max UTF8 expansion
            var byteCount = Encoding.UTF8.GetBytes(value.AsSpan(), stackBuffer);
            WriteVarInt(byteCount + 1); // +1 for compact encoding
            var span = _buffer.GetSpan(byteCount);
            stackBuffer.Slice(0, byteCount).CopyTo(span);
            _buffer.Advance(byteCount);
        }
        else
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            WriteVarInt(byteCount + 1); // +1 for compact encoding
            var span = _buffer.GetSpan(byteCount);
            Encoding.UTF8.GetBytes(value.AsSpan(), span);
            _buffer.Advance(byteCount);
        }
    }

    public void WriteBytes(byte[]? value)
    {
        if (value == null)
        {
            WriteInt32(-1);
            return;
        }

        WriteInt32(value.Length);
        var span = _buffer.GetSpan(value.Length);
        value.CopyTo(span);
        _buffer.Advance(value.Length);
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        WriteInt32(value.Length);
        var span = _buffer.GetSpan(value.Length);
        value.CopyTo(span);
        _buffer.Advance(value.Length);
    }

    public void WriteCompactBytes(byte[]? value)
    {
        if (value == null)
        {
            WriteVarInt(0);
            return;
        }

        WriteVarInt(value.Length + 1);
        var span = _buffer.GetSpan(value.Length);
        value.CopyTo(span);
        _buffer.Advance(value.Length);
    }

    /// <summary>
    /// Write raw bytes without any length prefix - used for special fields like Kafka "records"
    /// </summary>
    public void WriteRaw(byte[] value)
    {
        var span = _buffer.GetSpan(value.Length);
        value.CopyTo(span);
        _buffer.Advance(value.Length);
    }

    public void WriteRaw(ReadOnlySpan<byte> value)
    {
        var span = _buffer.GetSpan(value.Length);
        value.CopyTo(span);
        _buffer.Advance(value.Length);
    }

    public void WriteArray<T>(T[]? array, Action<T> writeElement)
    {
        if (array == null)
        {
            WriteInt32(-1);
            return;
        }

        WriteInt32(array.Length);
        foreach (var item in array)
        {
            writeElement(item);
        }
    }

    public void WriteCompactArray<T>(T[]? array, Action<T> writeElement)
    {
        if (array == null)
        {
            WriteVarInt(0);
            return;
        }

        WriteVarInt(array.Length + 1);
        foreach (var item in array)
        {
            writeElement(item);
        }
    }

    /// <summary>
    /// Write raw bytes without a length prefix (used for UUIDs, etc.)
    /// </summary>
    public void WriteRawBytes(byte[] bytes)
    {
        var span = _buffer.GetSpan(bytes.Length);
        bytes.CopyTo(span);
        _buffer.Advance(bytes.Length);
    }

    /// <summary>
    /// Write a UUID as 16 bytes in big-endian format (Kafka UUID encoding).
    /// Kafka UUIDs are stored as most significant 64 bits first, then least significant 64 bits.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUuid(Guid value)
    {
        var span = _buffer.GetSpan(16);
        // Kafka uses big-endian UUID format: 8 bytes MSB, 8 bytes LSB
        // .NET Guid.ToByteArray() uses mixed-endian (Windows), so we need to convert
        Span<byte> guidBytes = stackalloc byte[16];
        value.TryWriteBytes(guidBytes);

        // Convert from .NET's mixed-endian to Kafka's big-endian format
        // .NET format: bytes 0-3 (LE int32), 4-5 (LE int16), 6-7 (LE int16), 8-15 (BE)
        // Kafka format: 16 bytes in big-endian order
        span[0] = guidBytes[3];
        span[1] = guidBytes[2];
        span[2] = guidBytes[1];
        span[3] = guidBytes[0];
        span[4] = guidBytes[5];
        span[5] = guidBytes[4];
        span[6] = guidBytes[7];
        span[7] = guidBytes[6];
        guidBytes.Slice(8, 8).CopyTo(span.Slice(8));
        _buffer.Advance(16);
    }

    /// <summary>
    /// Returns the written data as a byte array.
    /// Note: This allocates a new array. For zero-copy access, use WrittenSpan.
    /// </summary>
    public byte[] ToArray() => _buffer.WrittenSpan.ToArray();

    /// <summary>
    /// Returns the written data as a read-only span (zero-copy).
    /// </summary>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.WrittenSpan;

    /// <summary>
    /// Returns the written data as a read-only memory (zero-copy).
    /// </summary>
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.WrittenMemory;

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            // ArrayBufferWriter doesn't need disposal but we keep the pattern
        }
    }
}

/// <summary>
/// Reader for Kafka protocol messages with proper decoding
/// </summary>
public sealed class KafkaProtocolReader
{
    private readonly byte[] _buffer;
    private readonly int _length;
    private int _position;

    public KafkaProtocolReader(byte[] buffer) : this(buffer, buffer.Length)
    {
    }

    public KafkaProtocolReader(byte[] buffer, int length)
    {
        _buffer = buffer;
        _length = length;
        _position = 0;
    }

    /// <summary>
    /// Creates a reader that views a slice of an existing buffer starting at
    /// <paramref name="offset"/>. Zero-copy: the caller retains ownership of
    /// <paramref name="buffer"/> and must ensure it outlives the reader.
    /// </summary>
    public KafkaProtocolReader(byte[] buffer, int offset, int length)
    {
        _buffer = buffer;
        _length = offset + length;
        _position = offset;
    }

    public int Position => _position;
    public int Remaining => _length - _position;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public sbyte ReadInt8()
    {
        if (_position >= _length)
            throw new InvalidDataException($"Cannot read Int8: position {_position} is at or beyond buffer length {_length}");
        return (sbyte)_buffer[_position++];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public short ReadInt16()
    {
        if (_position + 2 > _length)
            throw new InvalidDataException($"Cannot read Int16: position {_position} + 2 exceeds buffer length {_length}");
        var value = BinaryPrimitives.ReadInt16BigEndian(_buffer.AsSpan(_position, 2));
        _position += 2;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadInt32()
    {
        if (_position + 4 > _length)
            throw new InvalidDataException($"Cannot read Int32: position {_position} + 4 exceeds buffer length {_length}");
        var value = BinaryPrimitives.ReadInt32BigEndian(_buffer.AsSpan(_position, 4));
        _position += 4;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadInt64()
    {
        if (_position + 8 > _length)
            throw new InvalidDataException($"Cannot read Int64: position {_position} + 8 exceeds buffer length {_length}");
        var value = BinaryPrimitives.ReadInt64BigEndian(_buffer.AsSpan(_position, 8));
        _position += 8;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double ReadFloat64()
    {
        if (_position + 8 > _length)
            throw new InvalidDataException($"Cannot read Float64: position {_position} + 8 exceeds buffer length {_length}");
        var value = BinaryPrimitives.ReadInt64BigEndian(_buffer.AsSpan(_position, 8));
        _position += 8;
        return BitConverter.Int64BitsToDouble(value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint ReadUInt32()
    {
        if (_position + 4 > _length)
            throw new InvalidDataException($"Cannot read UInt32: position {_position} + 4 exceeds buffer length {_length}");
        var value = BinaryPrimitives.ReadUInt32BigEndian(_buffer.AsSpan(_position, 4));
        _position += 4;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ReadBoolean()
    {
        if (_position >= _length)
            throw new InvalidDataException($"Cannot read Boolean: position {_position} is at or beyond buffer length {_length}");
        return _buffer[_position++] != 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadVarInt()
    {
        if (_position >= _length)
            throw new InvalidDataException($"Cannot read VarInt: position {_position} is at or beyond buffer length {_length}");
        var (value, bytesRead) = KafkaProtocolPrimitives.ReadVarInt(_buffer.AsSpan(_position, _length - _position));
        _position += bytesRead;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadVarLong()
    {
        if (_position >= _length)
            throw new InvalidDataException($"Cannot read VarLong: position {_position} is at or beyond buffer length {_length}");
        var (value, bytesRead) = KafkaProtocolPrimitives.ReadVarLong(_buffer.AsSpan(_position, _length - _position));
        _position += bytesRead;
        return value;
    }

    public string? ReadString()
    {
        var length = ReadInt16();
        if (length < 0) return null;
        if (length == 0) return string.Empty;

        // Validate buffer bounds before reading
        if (_position + length > _length)
            throw new InvalidDataException($"String length {length} exceeds remaining buffer ({Remaining} bytes at position {_position})");

        var value = System.Text.Encoding.UTF8.GetString(_buffer, _position, length);
        _position += length;
        return value;
    }

    public string? ReadCompactString()
    {
        var length = ReadVarInt() - 1; // -1 for compact encoding
        if (length < 0) return null;
        if (length == 0) return string.Empty;

        // Validate buffer bounds before reading
        if (_position + length > _length)
            throw new InvalidDataException($"CompactString length {length} exceeds remaining buffer ({Remaining} bytes at position {_position})");

        var value = InternShortString(_buffer, _position, length);
        _position += length;
        return value;
    }

    // String interning cache for frequently repeated short strings (topic names,
    // client IDs). Avoids 500K+ UTF8.GetString allocations/sec when the same
    // 5-50 topic names appear in every request. Strings >64 bytes are not cached
    // (unlikely to be topic names, and the cache would grow unbounded).
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<int, string> s_stringCache = new();

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static string InternShortString(byte[] buffer, int offset, int length)
    {
        if (length > 64)
            return System.Text.Encoding.UTF8.GetString(buffer, offset, length);

        // Hash the raw bytes — cheaper than decoding to string first
        var hash = new HashCode();
        hash.AddBytes(buffer.AsSpan(offset, length));
        var key = hash.ToHashCode();

        if (s_stringCache.TryGetValue(key, out var cached))
        {
            // Verify: hash collision check (compare actual bytes)
            if (cached.Length <= length * 3) // rough UTF8 length check
            {
                return cached;
            }
        }

        var value = System.Text.Encoding.UTF8.GetString(buffer, offset, length);
        if (s_stringCache.Count < 10_000) // cap cache size
        {
            s_stringCache.TryAdd(key, value);
        }
        return value;
    }

    public byte[]? ReadBytes()
    {
        var length = ReadInt32();
        if (length < 0) return null;
        if (length == 0) return Array.Empty<byte>();

        // Validate buffer bounds before reading
        if (_position + length > _length)
            throw new InvalidDataException($"Bytes length {length} exceeds remaining buffer ({Remaining} bytes at position {_position})");

        var bytes = new byte[length];
        Buffer.BlockCopy(_buffer, _position, bytes, 0, length);
        _position += length;
        return bytes;
    }

    public byte[]? ReadCompactBytes()
    {
        var length = ReadVarInt() - 1;
        if (length < 0) return null;
        if (length == 0) return Array.Empty<byte>();

        // Validate buffer bounds before reading
        if (_position + length > _length)
            throw new InvalidDataException($"CompactBytes length {length} exceeds remaining buffer ({Remaining} bytes at position {_position})");

        var bytes = new byte[length];
        Buffer.BlockCopy(_buffer, _position, bytes, 0, length);
        _position += length;
        return bytes;
    }

    /// <summary>
    /// Zero-copy variant of <see cref="ReadCompactBytes"/> that returns a
    /// <see cref="ReadOnlyMemory{T}"/> slice into the reader's internal buffer
    /// instead of allocating + copying. The returned memory is valid as long as
    /// the underlying buffer is alive (typically until the pooled request buffer
    /// is returned in <c>ProcessKafkaRequestsAsync</c>'s finally block).
    /// </summary>
    public ReadOnlyMemory<byte> ReadCompactBytesMemory()
    {
        var length = ReadVarInt() - 1;
        if (length <= 0) return ReadOnlyMemory<byte>.Empty;

        if (_position + length > _length)
            throw new InvalidDataException($"CompactBytes length {length} exceeds remaining buffer ({Remaining} bytes at position {_position})");

        var memory = new ReadOnlyMemory<byte>(_buffer, _position, length);
        _position += length;
        return memory;
    }

    public T[] ReadArray<T>(Func<T> readElement)
    {
        var length = ReadInt32();
        if (length < 0) return Array.Empty<T>();

        var array = new T[length];
        for (int i = 0; i < length; i++)
        {
            array[i] = readElement();
        }

        return array;
    }

    public T[] ReadCompactArray<T>(Func<T> readElement)
    {
        var length = ReadVarInt() - 1;
        if (length < 0) return Array.Empty<T>();

        var array = new T[length];
        for (int i = 0; i < length; i++)
        {
            array[i] = readElement();
        }

        return array;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Skip(int bytes)
    {
        if (bytes < 0)
            throw new ArgumentOutOfRangeException(nameof(bytes), "Skip bytes cannot be negative");
        if (_position + bytes > _length)
            throw new InvalidDataException($"Cannot skip {bytes} bytes: only {_length - _position} bytes remaining at position {_position}");
        _position += bytes;
    }

    /// <summary>
    /// Read a fixed number of bytes from the buffer
    /// </summary>
    public byte[] ReadBytesFixed(int length)
    {
        if (length < 0) throw new ArgumentException("Length cannot be negative", nameof(length));
        if (length == 0) return Array.Empty<byte>();

        // Validate buffer bounds before reading
        if (_position + length > _length)
            throw new InvalidDataException($"Fixed bytes length {length} exceeds remaining buffer ({Remaining} bytes at position {_position})");

        var bytes = new byte[length];
        Buffer.BlockCopy(_buffer, _position, bytes, 0, length);
        _position += length;
        return bytes;
    }

    /// <summary>
    /// Zero-copy variant of <see cref="ReadBytesFixed"/> — returns a
    /// <see cref="ReadOnlyMemory{T}"/> slice into the internal buffer.
    /// </summary>
    public ReadOnlyMemory<byte> ReadBytesFixedMemory(int length)
    {
        if (length <= 0) return ReadOnlyMemory<byte>.Empty;

        if (_position + length > _length)
            throw new InvalidDataException($"Fixed bytes length {length} exceeds remaining buffer ({Remaining} bytes at position {_position})");

        var memory = new ReadOnlyMemory<byte>(_buffer, _position, length);
        _position += length;
        return memory;
    }

    /// <summary>
    /// Skip tagged fields in flexible protocol messages.
    /// Tagged fields are a variable-length list of field tag and value pairs.
    /// </summary>
    public void SkipTaggedFields()
    {
        var tagCount = ReadVarInt();
        for (int i = 0; i < tagCount; i++)
        {
            ReadVarInt(); // tag
            var size = ReadVarInt();
            Skip(size);
        }
    }

    /// <summary>
    /// Read a UUID from 16 bytes in big-endian format (Kafka UUID encoding).
    /// Kafka UUIDs are stored as most significant 64 bits first, then least significant 64 bits.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Guid ReadUuid()
    {
        if (_position + 16 > _length)
            throw new InvalidDataException($"UUID requires 16 bytes but only {Remaining} bytes remaining at position {_position}");

        var span = _buffer.AsSpan(_position, 16);

        // Convert from Kafka's big-endian format to .NET's mixed-endian format
        // Kafka format: 16 bytes in big-endian order
        // .NET format: bytes 0-3 (LE int32), 4-5 (LE int16), 6-7 (LE int16), 8-15 (BE)
        Span<byte> guidBytes = stackalloc byte[16];
        guidBytes[0] = span[3];
        guidBytes[1] = span[2];
        guidBytes[2] = span[1];
        guidBytes[3] = span[0];
        guidBytes[4] = span[5];
        guidBytes[5] = span[4];
        guidBytes[6] = span[7];
        guidBytes[7] = span[6];
        span.Slice(8, 8).CopyTo(guidBytes.Slice(8));

        _position += 16;
        return new Guid(guidBytes);
    }
}

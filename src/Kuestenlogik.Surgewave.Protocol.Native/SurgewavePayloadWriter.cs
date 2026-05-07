using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// Efficient binary writer for Surgewave protocol payloads
/// </summary>
public ref struct SurgewavePayloadWriter
{
    private readonly Span<byte> _buffer;
    private int _position;

    public SurgewavePayloadWriter(Span<byte> buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    public int Position => _position;
    public Span<byte> Written => _buffer[.._position];

    public void WriteInt8(sbyte value) => _buffer[_position++] = (byte)value;
    public void WriteUInt8(byte value) => _buffer[_position++] = value;

    public void WriteInt16(short value)
    {
        BinaryPrimitives.WriteInt16BigEndian(_buffer[_position..], value);
        _position += 2;
    }

    public void WriteUInt16(ushort value)
    {
        BinaryPrimitives.WriteUInt16BigEndian(_buffer[_position..], value);
        _position += 2;
    }

    public void WriteInt32(int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(_buffer[_position..], value);
        _position += 4;
    }

    public void WriteUInt32(uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(_buffer[_position..], value);
        _position += 4;
    }

    public void WriteInt64(long value)
    {
        BinaryPrimitives.WriteInt64BigEndian(_buffer[_position..], value);
        _position += 8;
    }

    public void WriteUInt64(ulong value)
    {
        BinaryPrimitives.WriteUInt64BigEndian(_buffer[_position..], value);
        _position += 8;
    }

    public void WriteString(string? value)
    {
        if (value == null)
        {
            WriteInt16(-1);
            return;
        }

        // Zero-allocation: encode directly to buffer
        var byteCount = Encoding.UTF8.GetByteCount(value);
        WriteInt16((short)byteCount);
        Encoding.UTF8.GetBytes(value.AsSpan(), _buffer.Slice(_position, byteCount));
        _position += byteCount;
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        WriteInt32(value.Length);
        value.CopyTo(_buffer[_position..]);
        _position += value.Length;
    }

    public void WriteRaw(ReadOnlySpan<byte> value)
    {
        value.CopyTo(_buffer[_position..]);
        _position += value.Length;
    }

    /// <summary>
    /// Advance the position by the specified number of bytes.
    /// Used when data is written directly to the buffer (e.g., via Encoding.GetBytes).
    /// </summary>
    public void Advance(int count) => _position += count;

    /// <summary>
    /// Write a boolean value (1 byte: 0=false, 1=true)
    /// </summary>
    public void WriteBoolean(bool value) => _buffer[_position++] = value ? (byte)1 : (byte)0;

    /// <summary>
    /// Write a nullable string with explicit null marker (1 byte prefix)
    /// </summary>
    public void WriteNullableString(string? value)
    {
        WriteBoolean(value != null);
        if (value != null)
        {
            WriteString(value);
        }
    }
}

using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// Efficient binary reader for Surgewave protocol payloads
/// </summary>
public ref struct SurgewavePayloadReader
{
    private readonly ReadOnlySpan<byte> _buffer;
    private int _position;

    public SurgewavePayloadReader(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    public int Position => _position;
    public int Remaining => _buffer.Length - _position;

    public sbyte ReadInt8() => (sbyte)_buffer[_position++];
    public byte ReadUInt8() => _buffer[_position++];

    public short ReadInt16()
    {
        var value = BinaryPrimitives.ReadInt16BigEndian(_buffer[_position..]);
        _position += 2;
        return value;
    }

    public ushort ReadUInt16()
    {
        var value = BinaryPrimitives.ReadUInt16BigEndian(_buffer[_position..]);
        _position += 2;
        return value;
    }

    public int ReadInt32()
    {
        var value = BinaryPrimitives.ReadInt32BigEndian(_buffer[_position..]);
        _position += 4;
        return value;
    }

    public uint ReadUInt32()
    {
        var value = BinaryPrimitives.ReadUInt32BigEndian(_buffer[_position..]);
        _position += 4;
        return value;
    }

    public long ReadInt64()
    {
        var value = BinaryPrimitives.ReadInt64BigEndian(_buffer[_position..]);
        _position += 8;
        return value;
    }

    public ulong ReadUInt64()
    {
        var value = BinaryPrimitives.ReadUInt64BigEndian(_buffer[_position..]);
        _position += 8;
        return value;
    }

    public string? ReadString()
    {
        var length = ReadInt16();
        if (length < 0) return null;

        var value = Encoding.UTF8.GetString(_buffer.Slice(_position, length));
        _position += length;
        return value;
    }

    public ReadOnlySpan<byte> ReadBytes()
    {
        var length = ReadInt32();
        var value = _buffer.Slice(_position, length);
        _position += length;
        return value;
    }

    /// <summary>
    /// Read raw bytes without a length prefix (length is provided externally)
    /// </summary>
    public ReadOnlySpan<byte> ReadRaw(int length)
    {
        var value = _buffer.Slice(_position, length);
        _position += length;
        return value;
    }

    public void Skip(int bytes) => _position += bytes;

    /// <summary>
    /// Read a boolean value (1 byte: 0=false, non-zero=true)
    /// </summary>
    public bool ReadBoolean() => _buffer[_position++] != 0;

    /// <summary>
    /// Read a nullable string with explicit null marker (1 byte prefix)
    /// </summary>
    public string? ReadNullableString()
    {
        var hasValue = ReadBoolean();
        return hasValue ? ReadString() : null;
    }
}

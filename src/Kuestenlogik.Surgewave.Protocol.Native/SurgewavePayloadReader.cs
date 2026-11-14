using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// Efficient binary reader for Surgewave protocol payloads
/// </summary>
/// <remarks>
/// A read past the end of the payload, or of a length the payload does not have, is a
/// <see cref="SurgewavePayloadException"/>: the payload is not what the reader was told it is.
/// </remarks>
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

    public sbyte ReadInt8() => (sbyte)Take(1)[0];
    public byte ReadUInt8() => Take(1)[0];

    public short ReadInt16() => BinaryPrimitives.ReadInt16BigEndian(Take(2));

    public ushort ReadUInt16() => BinaryPrimitives.ReadUInt16BigEndian(Take(2));

    public int ReadInt32() => BinaryPrimitives.ReadInt32BigEndian(Take(4));

    public uint ReadUInt32() => BinaryPrimitives.ReadUInt32BigEndian(Take(4));

    public long ReadInt64() => BinaryPrimitives.ReadInt64BigEndian(Take(8));

    public ulong ReadUInt64() => BinaryPrimitives.ReadUInt64BigEndian(Take(8));

    public string? ReadString()
    {
        var length = ReadInt16();
        if (length < 0) return null;

        return Encoding.UTF8.GetString(Take(length));
    }

    public ReadOnlySpan<byte> ReadBytes() => Take(ReadInt32());

    /// <summary>
    /// Read raw bytes without a length prefix (length is provided externally)
    /// </summary>
    public ReadOnlySpan<byte> ReadRaw(int length) => Take(length);

    public void Skip(int bytes) => Take(bytes);

    /// <summary>
    /// Read a boolean value (1 byte: 0=false, non-zero=true)
    /// </summary>
    public bool ReadBoolean() => Take(1)[0] != 0;

    /// <summary>
    /// Read a nullable string with explicit null marker (1 byte prefix)
    /// </summary>
    public string? ReadNullableString()
    {
        var hasValue = ReadBoolean();
        return hasValue ? ReadString() : null;
    }

    // The next count bytes, and the position behind them. One comparison covers a negative count and one past the end.
    private ReadOnlySpan<byte> Take(int count)
    {
        if ((uint)count > (uint)(_buffer.Length - _position))
            throw SurgewavePayloadException.At(_position, count, _buffer.Length - _position);

        var taken = _buffer.Slice(_position, count);
        _position += count;
        return taken;
    }
}

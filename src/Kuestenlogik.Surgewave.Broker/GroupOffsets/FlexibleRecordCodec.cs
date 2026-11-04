using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Broker.GroupOffsets;

/// <summary>
/// Kafka's record encoding for the group-state records of <c>__consumer_offsets</c> (#219): keys not flexible — the
/// int16 key version, then int16-length strings — and values led by their int16 version, then flexible — compact
/// strings and arrays, unsigned varints, a tagged-field section at the end.
/// </summary>
internal static class FlexibleRecordCodec
{
    /// <summary>A key: its version, then the strings that name the record.</summary>
    public static byte[] Key(short version, string first, string? second = null)
    {
        var size = 2 + 2 + Encoding.UTF8.GetByteCount(first) + (second is null ? 0 : 2 + Encoding.UTF8.GetByteCount(second));
        var buffer = new byte[size];
        BinaryPrimitives.WriteInt16BigEndian(buffer, version);
        var at = 2 + WriteKeyString(buffer.AsSpan(2), first);
        if (second is not null)
            WriteKeyString(buffer.AsSpan(at), second);
        return buffer;
    }

    /// <summary>The version of a key.</summary>
    public static short KeyVersion(ReadOnlySpan<byte> key) => BinaryPrimitives.ReadInt16BigEndian(key);

    /// <summary>The key's second string, the member of a member's record.</summary>
    public static string? SecondKeyString(ReadOnlySpan<byte> key)
    {
        var at = 2;
        _ = ReadKeyString(key, ref at);
        return at < key.Length ? ReadKeyString(key, ref at) : null;
    }

    /// <summary>A value: its version 0, what <paramref name="write"/> writes, then the tagged fields.</summary>
    public static byte[] Value(Action<FlexibleWriter> write, IReadOnlyList<(uint Tag, byte[] Data)>? taggedFields = null)
    {
        var writer = new FlexibleWriter();
        writer.Int16(0);
        write(writer);
        writer.TaggedFields(taggedFields ?? []);
        return writer.ToArray();
    }

    private static int WriteKeyString(Span<byte> span, string value)
    {
        var length = Encoding.UTF8.GetBytes(value, span[2..]);
        BinaryPrimitives.WriteInt16BigEndian(span, (short)length);
        return 2 + length;
    }

    private static string? ReadKeyString(ReadOnlySpan<byte> span, ref int at)
    {
        var length = BinaryPrimitives.ReadInt16BigEndian(span[at..]);
        at += 2;
        if (length < 0)
            return null;
        var value = Encoding.UTF8.GetString(span.Slice(at, length));
        at += length;
        return value;
    }
}

/// <summary>Writes a flexible value.</summary>
internal sealed class FlexibleWriter
{
    private readonly List<byte> _bytes = [];

    public byte[] ToArray() => [.. _bytes];

    public void Int8(sbyte value) => _bytes.Add((byte)value);

    public void Int16(short value)
    {
        Span<byte> b = stackalloc byte[2];
        BinaryPrimitives.WriteInt16BigEndian(b, value);
        _bytes.AddRange(b);
    }

    public void Int32(int value)
    {
        Span<byte> b = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(b, value);
        _bytes.AddRange(b);
    }

    public void Int64(long value)
    {
        Span<byte> b = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(b, value);
        _bytes.AddRange(b);
    }

    public void Uuid(Guid value)
    {
        Span<byte> b = stackalloc byte[16];
        value.TryWriteBytes(b, bigEndian: true, out _);
        _bytes.AddRange(b);
    }

    public void UnsignedVarint(uint value)
    {
        while ((value & ~0x7Fu) != 0)
        {
            _bytes.Add((byte)((value & 0x7F) | 0x80));
            value >>= 7;
        }
        _bytes.Add((byte)value);
    }

    public void String(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        UnsignedVarint((uint)bytes.Length + 1);
        _bytes.AddRange(bytes);
    }

    public void NullableString(string? value)
    {
        if (value is null)
            UnsignedVarint(0);
        else
            String(value);
    }

    public void Array<T>(IReadOnlyCollection<T> items, Action<FlexibleWriter, T> write)
    {
        UnsignedVarint((uint)items.Count + 1);
        foreach (var item in items)
            write(this, item);
    }

    public void TaggedFields(IReadOnlyList<(uint Tag, byte[] Data)> fields)
    {
        UnsignedVarint((uint)fields.Count);
        foreach (var (tag, data) in fields.OrderBy(f => f.Tag))
        {
            UnsignedVarint(tag);
            UnsignedVarint((uint)data.Length);
            _bytes.AddRange(data);
        }
    }
}

/// <summary>Reads a flexible value, past its version.</summary>
internal ref struct FlexibleReader(ReadOnlySpan<byte> value, int at = 2)
{
    private readonly ReadOnlySpan<byte> _value = value;
    private int _at = at;

    public sbyte Int8() => (sbyte)_value[_at++];

    public int Int32()
    {
        var v = BinaryPrimitives.ReadInt32BigEndian(_value[_at..]);
        _at += 4;
        return v;
    }

    public short Int16()
    {
        var v = BinaryPrimitives.ReadInt16BigEndian(_value[_at..]);
        _at += 2;
        return v;
    }

    public long Int64()
    {
        var v = BinaryPrimitives.ReadInt64BigEndian(_value[_at..]);
        _at += 8;
        return v;
    }

    public Guid Uuid()
    {
        var v = new Guid(_value.Slice(_at, 16), bigEndian: true);
        _at += 16;
        return v;
    }

    public uint UnsignedVarint()
    {
        uint result = 0;
        var shift = 0;
        byte b;
        do
        {
            b = _value[_at++];
            result |= (uint)(b & 0x7F) << shift;
            shift += 7;
        }
        while ((b & 0x80) != 0);
        return result;
    }

    public string String() => NullableString() ?? string.Empty;

    public string? NullableString()
    {
        var length = (int)UnsignedVarint() - 1;
        if (length < 0)
            return null;
        var s = Encoding.UTF8.GetString(_value.Slice(_at, length));
        _at += length;
        return s;
    }

    public int ArrayCount() => Math.Max((int)UnsignedVarint() - 1, 0);

    public List<string> StringArray()
    {
        var count = ArrayCount();
        var items = new List<string>(count);
        for (var i = 0; i < count; i++)
            items.Add(String());
        return items;
    }

    /// <summary>The tagged fields, by tag.</summary>
    public Dictionary<uint, byte[]> TaggedFields()
    {
        var fields = new Dictionary<uint, byte[]>();
        var count = UnsignedVarint();
        for (var f = 0; f < count; f++)
        {
            var tag = UnsignedVarint();
            var size = (int)UnsignedVarint();
            fields[tag] = _value.Slice(_at, size).ToArray();
            _at += size;
        }
        return fields;
    }
}

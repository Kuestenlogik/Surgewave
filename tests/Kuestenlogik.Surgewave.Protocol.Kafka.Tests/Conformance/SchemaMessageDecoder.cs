using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests.Conformance;

/// <summary>
/// Reads a message the way Kafka's generated code parses it, straight from the schema, and returns the values it
/// found by field name. Used on the broker's responses: what a Kafka client would read from them.
/// </summary>
internal sealed class SchemaMessageDecoder
{
    private readonly byte[] _data;
    private int _position;

    private SchemaMessageDecoder(byte[] data, int position)
    {
        _data = data;
        _position = position;
    }

    public int Remaining => _data.Length - _position;

    /// <summary>Decodes <paramref name="data"/> from <paramref name="offset"/>; returns the values and the bytes left over.</summary>
    public static (ExpectedStruct Values, int Remaining) Decode(KafkaMessageSchema schema, short version, byte[] data, int offset)
    {
        var decoder = new SchemaMessageDecoder(data, offset);
        var values = decoder.ReadStruct(schema.Fields, version, schema.FlexibleVersions.Contains(version));
        return (values, decoder.Remaining);
    }

    private ExpectedStruct ReadStruct(IReadOnlyList<SchemaField> fields, short version, bool flexible)
    {
        var values = new ExpectedStruct();
        foreach (var field in fields)
        {
            if (!field.Versions.Contains(version) || (flexible && field.IsTagged(version)))
                continue;
            ReadField(field, version, flexible, values);
        }

        if (!flexible)
            return values;

        var tagCount = ReadUnsignedVarInt();
        for (var i = 0; i < tagCount; i++)
        {
            var tag = ReadUnsignedVarInt();
            var size = ReadUnsignedVarInt();
            var end = _position + size;
            var field = fields.FirstOrDefault(f => f.Tag == tag && f.IsTagged(version));
            if (field is null)
            {
                _position = end; // a tag this version does not define: skipped, as a client skips it
                continue;
            }
            ReadField(field, version, flexible, values);
            if (_position != end)
                throw new InvalidDataException($"tagged field {tag} declared {size} bytes, its value took {size - (end - _position)}");
        }
        return values;
    }

    private void ReadField(SchemaField field, short version, bool flexible, ExpectedStruct values)
    {
        var nullable = field.NullableVersions.Contains(version);
        if (field.IsArray)
        {
            int count = flexible ? ReadUnsignedVarInt() - 1 : ReadInt32();
            if (count < 0)
            {
                if (!nullable)
                    throw new InvalidDataException($"non-nullable array {field.Name} was null");
                return;
            }
            if (count > Remaining)
                throw new InvalidDataException($"array {field.Name} claims {count} elements with {Remaining} bytes left");
            var elements = new List<object>(count);
            for (var i = 0; i < count; i++)
            {
                elements.Add(field.StructFields is not null
                    ? ReadStruct(field.StructFields, version, flexible)
                    : ReadPrimitive(field.ElementType!, field.Name, nullable: false, flexible)!);
            }
            values.Fields[field.Name] = new ExpectedArray(elements);
            return;
        }

        if (field.IsStruct)
        {
            if (nullable && (sbyte)ReadByte() < 0)
                return;
            values.Fields[field.Name] = ReadStruct(field.StructFields!, version, flexible);
            return;
        }

        var value = ReadPrimitive(field.Type, field.Name, nullable, flexible);
        if (value is not null)
            values.Fields[field.Name] = value;
    }

    private object? ReadPrimitive(string type, string name, bool nullable, bool flexible)
    {
        switch (type)
        {
            case "int8": return (long)(sbyte)ReadByte();
            case "int16": return (long)BinaryPrimitives.ReadInt16BigEndian(Take(2));
            case "uint16": return (long)BinaryPrimitives.ReadUInt16BigEndian(Take(2));
            case "int32": return (long)ReadInt32();
            case "uint32": return (long)BinaryPrimitives.ReadUInt32BigEndian(Take(4));
            case "int64": return BinaryPrimitives.ReadInt64BigEndian(Take(8));
            case "float64": return BinaryPrimitives.ReadDoubleBigEndian(Take(8));
            case "bool": return ReadByte() != 0;
            case "uuid": return new Guid(Take(16), bigEndian: true);
            case "string":
            {
                int length = flexible ? ReadUnsignedVarInt() - 1 : BinaryPrimitives.ReadInt16BigEndian(Take(2));
                if (length < 0)
                {
                    if (!nullable)
                        throw new InvalidDataException($"non-nullable string {name} was null");
                    return null;
                }
                return Encoding.UTF8.GetString(Take(length));
            }
            case "bytes":
            case "records":
            {
                int length = flexible ? ReadUnsignedVarInt() - 1 : ReadInt32();
                if (length < 0)
                {
                    if (!nullable && type == "bytes")
                        throw new InvalidDataException($"non-nullable bytes {name} was null");
                    return null;
                }
                var bytes = Take(length).ToArray();
                return type == "records" ? ExpectedStruct.Unchecked : bytes;
            }
            default:
                throw new NotSupportedException($"Schema type '{type}' of field '{name}'");
        }
    }

    private ReadOnlySpan<byte> Take(int count)
    {
        if (count < 0 || count > Remaining)
            throw new InvalidDataException($"needs {count} bytes, {Remaining} left at position {_position}");
        var span = _data.AsSpan(_position, count);
        _position += count;
        return span;
    }

    private byte ReadByte() => Take(1)[0];

    private int ReadInt32() => BinaryPrimitives.ReadInt32BigEndian(Take(4));

    private int ReadUnsignedVarInt()
    {
        int value = 0, shift = 0;
        while (true)
        {
            var b = ReadByte();
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return value;
            shift += 7;
            if (shift > 28)
                throw new InvalidDataException("varint too long");
        }
    }
}

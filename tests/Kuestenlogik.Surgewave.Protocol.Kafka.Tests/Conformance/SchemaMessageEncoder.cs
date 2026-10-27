using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests.Conformance;

/// <summary>
/// Writes a message the way Kafka's generated code serializes it, straight from the schema: field order, compact
/// versus classic encodings, nullable markers, and the tagged-field sections of the message and of every struct in
/// flexible versions. Every field gets a value no other field of the message has, so a codec that reads a field
/// from the wrong place shows up as a wrong value, not only as an exception.
/// </summary>
internal sealed class SchemaMessageEncoder
{
    private int _sequence;

    /// <summary>The body of <paramref name="schema"/> at <paramref name="version"/>, and the value written for each field.</summary>
    public static (byte[] Body, ExpectedStruct Expected) Encode(KafkaMessageSchema schema, short version)
    {
        var encoder = new SchemaMessageEncoder();
        var output = new MemoryStream();
        var expected = new ExpectedStruct();
        encoder.WriteStruct(schema.Fields, version, schema.FlexibleVersions.Contains(version), output, expected);
        return (output.ToArray(), expected);
    }

    private void WriteStruct(IReadOnlyList<SchemaField> fields, short version, bool flexible, MemoryStream output, ExpectedStruct expected)
    {
        var tagged = new List<SchemaField>();
        foreach (var field in fields)
        {
            if (!field.Versions.Contains(version))
                continue;
            if (flexible && field.IsTagged(version))
            {
                tagged.Add(field);
                continue;
            }
            WriteField(field, version, flexible, output, expected);
        }

        if (!flexible)
            return;

        WriteUnsignedVarInt(output, tagged.Count);
        foreach (var field in tagged.OrderBy(f => f.Tag))
        {
            var data = new MemoryStream();
            WriteField(field, version, flexible, data, expected);
            WriteUnsignedVarInt(output, field.Tag);
            WriteUnsignedVarInt(output, (int)data.Length);
            data.WriteTo(output);
        }
    }

    private void WriteField(SchemaField field, short version, bool flexible, MemoryStream output, ExpectedStruct expected)
    {
        if (field.IsArray)
        {
            // One element: enough to see each element's layout, and its values.
            if (flexible) WriteUnsignedVarInt(output, 2); else WriteInt32(output, 1);
            if (field.StructFields is not null)
            {
                var element = new ExpectedStruct();
                WriteStruct(field.StructFields, version, flexible, output, element);
                expected.Fields[field.Name] = new ExpectedArray([element]);
            }
            else
            {
                expected.Fields[field.Name] = new ExpectedArray([WritePrimitive(field.ElementType!, field.Name, flexible, output)]);
            }
            return;
        }

        if (field.IsStruct)
        {
            if (field.NullableVersions.Contains(version))
                output.WriteByte(1); // present
            var inner = new ExpectedStruct();
            WriteStruct(field.StructFields!, version, flexible, output, inner);
            expected.Fields[field.Name] = inner;
            return;
        }

        expected.Fields[field.Name] = WritePrimitive(field.Type, field.Name, flexible, output);
    }

    private object WritePrimitive(string type, string name, bool flexible, MemoryStream output)
    {
        var n = ++_sequence;
        switch (type)
        {
            case "int8":
                output.WriteByte((byte)(n % 100 + 1));
                return (long)(n % 100 + 1);
            case "int16":
                WriteInt16(output, (short)(1000 + n));
                return (long)(1000 + n);
            case "uint16":
                WriteInt16(output, (short)(2000 + n));
                return (long)(2000 + n);
            case "int32":
                WriteInt32(output, 100_000 + n);
                return (long)(100_000 + n);
            case "uint32":
                WriteInt32(output, 200_000 + n);
                return (long)(200_000 + n);
            case "int64":
            {
                var value = 10_000_000_000L + n;
                Span<byte> b = stackalloc byte[8];
                BinaryPrimitives.WriteInt64BigEndian(b, value);
                output.Write(b);
                return value;
            }
            case "float64":
            {
                var value = n + 0.5;
                Span<byte> b = stackalloc byte[8];
                BinaryPrimitives.WriteDoubleBigEndian(b, value);
                output.Write(b);
                return value;
            }
            case "bool":
                output.WriteByte(1);
                return true;
            case "uuid":
            {
                var bytes = new byte[16];
                BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(12), n);
                bytes[0] = 0x5a;
                output.Write(bytes);
                return new Guid(bytes, bigEndian: true);
            }
            case "string":
            {
                var value = $"{name}-{n}";
                var utf8 = Encoding.UTF8.GetBytes(value);
                if (flexible) WriteUnsignedVarInt(output, utf8.Length + 1); else WriteInt16(output, (short)utf8.Length);
                output.Write(utf8);
                return value;
            }
            case "bytes":
            {
                var value = new[] { (byte)n, (byte)(n + 1), (byte)(n + 2) };
                if (flexible) WriteUnsignedVarInt(output, value.Length + 1); else WriteInt32(output, value.Length);
                output.Write(value);
                return value;
            }
            case "records":
                // An empty record set: the layout around it is what is checked here, not record batches.
                if (flexible) WriteUnsignedVarInt(output, 1); else WriteInt32(output, 0);
                return ExpectedStruct.Unchecked;
            default:
                throw new NotSupportedException($"Schema type '{type}' of field '{name}'");
        }
    }

    internal static void WriteInt16(Stream output, short value)
    {
        Span<byte> b = stackalloc byte[2];
        BinaryPrimitives.WriteInt16BigEndian(b, value);
        output.Write(b);
    }

    internal static void WriteInt32(Stream output, int value)
    {
        Span<byte> b = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(b, value);
        output.Write(b);
    }

    internal static void WriteUnsignedVarInt(Stream output, int value)
    {
        var v = (uint)value;
        while ((v & ~0x7Fu) != 0)
        {
            output.WriteByte((byte)((v & 0x7F) | 0x80));
            v >>= 7;
        }
        output.WriteByte((byte)v);
    }
}

/// <summary>The values written for a message or struct, by field name.</summary>
internal sealed class ExpectedStruct
{
    /// <summary>A field whose content is not compared (record sets).</summary>
    public static readonly object Unchecked = new();

    public Dictionary<string, object> Fields { get; } = new(StringComparer.Ordinal);
}

/// <summary>The elements written for an array field.</summary>
internal sealed record ExpectedArray(IReadOnlyList<object> Elements);

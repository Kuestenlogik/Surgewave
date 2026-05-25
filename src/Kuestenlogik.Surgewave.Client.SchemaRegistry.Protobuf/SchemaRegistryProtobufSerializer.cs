using System.Collections.Concurrent;
using Google.Protobuf;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Protobuf;

/// <summary>
/// Protobuf serializer with schema registry integration.
/// Uses Confluent wire format: [0x00][4-byte schema ID][varint message index][Protobuf payload]
/// </summary>
/// <typeparam name="T">The Protobuf message type to serialize.</typeparam>
public sealed class SchemaRegistryProtobufSerializer<T> : IAsyncSerializer<T> where T : IMessage<T>
{
    private readonly ProtobufSerializerConfig _config;
    private readonly ConcurrentDictionary<string, int> _schemaIdCache = new();

    /// <summary>
    /// Create a new schema registry Protobuf serializer.
    /// </summary>
    /// <param name="config">Serializer configuration.</param>
    public SchemaRegistryProtobufSerializer(ProtobufSerializerConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <inheritdoc />
    public async ValueTask<byte[]?> SerializeAsync(T? data, string topic, CancellationToken cancellationToken = default)
    {
        if (data == null)
            return null;

        // Get or register schema
        var schemaId = await GetOrRegisterSchemaAsync(topic, data, cancellationToken);

        // Serialize the data
        using var stream = new MemoryStream();

        // Write wire format header
        stream.WriteByte(SchemaRegistryWireFormat.MagicByte);
        stream.WriteByte((byte)(schemaId >> 24));
        stream.WriteByte((byte)(schemaId >> 16));
        stream.WriteByte((byte)(schemaId >> 8));
        stream.WriteByte((byte)schemaId);

        // Write message index as varint (0 for single message type)
        WriteVarint(stream, 0);

        // Write Protobuf payload
        data.WriteTo(stream);

        return stream.ToArray();
    }

    private async Task<int> GetOrRegisterSchemaAsync(T data, string topic, CancellationToken cancellationToken)
    {
        var recordName = data.Descriptor.FullName;
        var subject = _config.SubjectNameStrategy.GetSubjectName(topic, _config.IsKey, recordName);

        if (_schemaIdCache.TryGetValue(subject, out var cachedId))
            return cachedId;

        // Generate proto schema from descriptor
        var schemaString = GenerateProtoSchema(data);

        if (_config.AutoRegisterSchemas)
        {
            var result = await _config.SchemaRegistry.RegisterSchemaAsync(subject, schemaString, "PROTOBUF", cancellationToken);
            _schemaIdCache[subject] = result.SchemaId;
            return result.SchemaId;
        }

        // Look up existing schema
        var versions = await _config.SchemaRegistry.GetSubjectVersionsAsync(subject, cancellationToken);
        if (versions.Count == 0)
            throw new InvalidOperationException($"No schema registered for subject '{subject}' and auto-registration is disabled");

        var latestVersion = versions.Max();
        var schemaInfo = await _config.SchemaRegistry.GetSchemaByVersionAsync(subject, latestVersion, cancellationToken);
        if (schemaInfo == null)
            throw new InvalidOperationException($"Schema not found for subject '{subject}' version {latestVersion}");

        _schemaIdCache[subject] = schemaInfo.Id;
        return schemaInfo.Id;
    }

    private Task<int> GetOrRegisterSchemaAsync(string topic, T data, CancellationToken cancellationToken)
        => GetOrRegisterSchemaAsync(data, topic, cancellationToken);

    private static string GenerateProtoSchema(T message)
    {
        var descriptor = message.Descriptor;
        var sb = new System.Text.StringBuilder();

        sb.AppendLine("syntax = \"proto3\";");

        if (!string.IsNullOrEmpty(descriptor.File.Package))
            sb.AppendLine($"package {descriptor.File.Package};");

        sb.AppendLine();
        sb.AppendLine($"message {descriptor.Name} {{");

        foreach (var field in descriptor.Fields.InFieldNumberOrder())
        {
            var typeName = GetProtoTypeName(field);
            sb.AppendLine($"  {typeName} {field.JsonName} = {field.FieldNumber};");
        }

        sb.AppendLine("}");

        return sb.ToString();
    }

    private static string GetProtoTypeName(Google.Protobuf.Reflection.FieldDescriptor field)
    {
        return field.FieldType switch
        {
            Google.Protobuf.Reflection.FieldType.Double => "double",
            Google.Protobuf.Reflection.FieldType.Float => "float",
            Google.Protobuf.Reflection.FieldType.Int64 => "int64",
            Google.Protobuf.Reflection.FieldType.UInt64 => "uint64",
            Google.Protobuf.Reflection.FieldType.Int32 => "int32",
            Google.Protobuf.Reflection.FieldType.Fixed64 => "fixed64",
            Google.Protobuf.Reflection.FieldType.Fixed32 => "fixed32",
            Google.Protobuf.Reflection.FieldType.Bool => "bool",
            Google.Protobuf.Reflection.FieldType.String => "string",
            Google.Protobuf.Reflection.FieldType.Bytes => "bytes",
            Google.Protobuf.Reflection.FieldType.UInt32 => "uint32",
            Google.Protobuf.Reflection.FieldType.SFixed32 => "sfixed32",
            Google.Protobuf.Reflection.FieldType.SFixed64 => "sfixed64",
            Google.Protobuf.Reflection.FieldType.SInt32 => "sint32",
            Google.Protobuf.Reflection.FieldType.SInt64 => "sint64",
            Google.Protobuf.Reflection.FieldType.Message => field.MessageType.FullName,
            Google.Protobuf.Reflection.FieldType.Enum => field.EnumType.FullName,
            _ => "bytes"
        };
    }

    private static void WriteVarint(Stream stream, int value)
    {
        while (value > 0x7F)
        {
            stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value >>= 7;
        }
        stream.WriteByte((byte)value);
    }
}

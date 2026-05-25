using System.Collections.Concurrent;
using System.Text.Json;
using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Avro;

/// <summary>
/// Avro serializer with schema registry integration.
/// Uses Confluent wire format: [0x00][4-byte schema ID][Avro payload]
/// </summary>
/// <remarks>
/// This implementation uses JSON as an intermediate format for Avro serialization.
/// For high-performance binary Avro, consider using the Chr.Avro.Binary package directly.
/// </remarks>
/// <typeparam name="T">The type to serialize.</typeparam>
public sealed class SchemaRegistryAvroSerializer<T> : IAsyncSerializer<T>
{
    private readonly AvroSerializerConfig _config;
    private readonly ConcurrentDictionary<string, int> _schemaIdCache = new();
    private readonly SchemaBuilder _schemaBuilder = new();
    private readonly JsonSchemaWriter _schemaWriter = new();
    private readonly Lazy<Chr.Avro.Abstract.Schema> _schema;
    private readonly Func<T, byte[]>? _customSerializer;

    /// <summary>
    /// Create a new schema registry Avro serializer using JSON encoding.
    /// </summary>
    /// <param name="config">Serializer configuration.</param>
    public SchemaRegistryAvroSerializer(AvroSerializerConfig config)
        : this(config, null)
    {
    }

    /// <summary>
    /// Create a new schema registry Avro serializer with a custom binary serializer.
    /// </summary>
    /// <param name="config">Serializer configuration.</param>
    /// <param name="customSerializer">Optional custom serializer for binary Avro encoding.</param>
    public SchemaRegistryAvroSerializer(AvroSerializerConfig config, Func<T, byte[]>? customSerializer)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _customSerializer = customSerializer;
        _schema = new Lazy<Chr.Avro.Abstract.Schema>(() => _schemaBuilder.BuildSchema<T>());
    }

    /// <inheritdoc />
    public async ValueTask<byte[]?> SerializeAsync(T? data, string topic, CancellationToken cancellationToken = default)
    {
        if (data == null)
            return null;

        // Get or register schema
        var schemaId = await GetOrRegisterSchemaAsync(topic, cancellationToken);

        // Serialize the data
        byte[] payload;
        if (_customSerializer != null)
        {
            payload = _customSerializer(data);
        }
        else
        {
            // Use JSON encoding as fallback (compatible with Avro JSON encoding)
            var json = JsonSerializer.SerializeToUtf8Bytes(data);
            payload = json;
        }

        // Build result with wire format header
        var result = new byte[SchemaRegistryWireFormat.HeaderSize + payload.Length];
        SchemaRegistryWireFormat.WriteHeader(result, schemaId);
        payload.CopyTo(result.AsSpan(SchemaRegistryWireFormat.HeaderSize));

        return result;
    }

    private async Task<int> GetOrRegisterSchemaAsync(string topic, CancellationToken cancellationToken)
    {
        var recordName = GetRecordName();
        var subject = _config.SubjectNameStrategy.GetSubjectName(topic, _config.IsKey, recordName);

        if (_schemaIdCache.TryGetValue(subject, out var cachedId))
            return cachedId;

        var schemaString = _schemaWriter.Write(_schema.Value);

        if (_config.AutoRegisterSchemas)
        {
            var result = await _config.SchemaRegistry.RegisterSchemaAsync(subject, schemaString, "AVRO", cancellationToken);
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

    private string? GetRecordName()
    {
        if (_schema.Value is RecordSchema recordSchema)
            return string.IsNullOrEmpty(recordSchema.Namespace)
                ? recordSchema.Name
                : $"{recordSchema.Namespace}.{recordSchema.Name}";

        return typeof(T).FullName;
    }
}

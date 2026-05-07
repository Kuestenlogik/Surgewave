using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Serialization;
using NJsonSchema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Json;

/// <summary>
/// JSON serializer with JSON Schema registry integration.
/// Uses Confluent wire format: [0x00][4-byte schema ID][JSON payload]
/// </summary>
/// <typeparam name="T">The type to serialize.</typeparam>
public sealed class SchemaRegistryJsonSerializer<T> : IAsyncSerializer<T>
{
    private readonly JsonSchemaSerializerConfig _config;
    private readonly ConcurrentDictionary<string, (int schemaId, NJsonSchema.JsonSchema schema)> _schemaCache = new();
    private readonly Lazy<NJsonSchema.JsonSchema> _generatedSchema;

    /// <summary>
    /// Create a new schema registry JSON serializer.
    /// </summary>
    /// <param name="config">Serializer configuration.</param>
    public SchemaRegistryJsonSerializer(JsonSchemaSerializerConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _generatedSchema = new Lazy<NJsonSchema.JsonSchema>(() => NJsonSchema.JsonSchema.FromType<T>());
    }

    /// <inheritdoc />
    public async ValueTask<byte[]?> SerializeAsync(T? data, string topic, CancellationToken cancellationToken = default)
    {
        if (data == null)
            return null;

        // Get or register schema
        var (schemaId, schema) = await GetOrRegisterSchemaAsync(topic, cancellationToken);

        // Serialize to JSON
        var json = JsonSerializer.Serialize(data, _config.JsonOptions);

        // Optionally validate against schema
        if (_config.ValidateOnSerialize && schema != null)
        {
            var errors = schema.Validate(json);
            if (errors.Count > 0)
                throw new InvalidOperationException($"JSON validation failed: {string.Join(", ", errors.Select(e => e.ToString()))}");
        }

        var jsonBytes = Encoding.UTF8.GetBytes(json);

        // Build result with wire format header
        var result = new byte[SchemaRegistryWireFormat.HeaderSize + jsonBytes.Length];
        SchemaRegistryWireFormat.WriteHeader(result, schemaId);
        jsonBytes.CopyTo(result.AsSpan(SchemaRegistryWireFormat.HeaderSize));

        return result;
    }

    private async Task<(int schemaId, NJsonSchema.JsonSchema? schema)> GetOrRegisterSchemaAsync(string topic, CancellationToken cancellationToken)
    {
        var recordName = typeof(T).FullName;
        var subject = _config.SubjectNameStrategy.GetSubjectName(topic, _config.IsKey, recordName);

        if (_schemaCache.TryGetValue(subject, out var cached))
            return cached;

        var schemaString = _generatedSchema.Value.ToJson();

        if (_config.AutoRegisterSchemas)
        {
            var result = await _config.SchemaRegistry.RegisterSchemaAsync(subject, schemaString, "JSON", cancellationToken);
            var entry = (result.SchemaId, _generatedSchema.Value);
            _schemaCache[subject] = entry;
            return entry;
        }

        // Look up existing schema
        var versions = await _config.SchemaRegistry.GetSubjectVersionsAsync(subject, cancellationToken);
        if (versions.Count == 0)
            throw new InvalidOperationException($"No schema registered for subject '{subject}' and auto-registration is disabled");

        var latestVersion = versions.Max();
        var schemaInfo = await _config.SchemaRegistry.GetSchemaByVersionAsync(subject, latestVersion, cancellationToken);
        if (schemaInfo == null)
            throw new InvalidOperationException($"Schema not found for subject '{subject}' version {latestVersion}");

        var registeredSchema = await NJsonSchema.JsonSchema.FromJsonAsync(schemaInfo.SchemaString, cancellationToken);
        var cacheEntry = (schemaInfo.Id, registeredSchema);
        _schemaCache[subject] = cacheEntry;
        return cacheEntry;
    }
}

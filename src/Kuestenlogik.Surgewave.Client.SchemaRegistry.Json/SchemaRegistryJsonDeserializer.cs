using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Serialization;
using NJsonSchema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Json;

/// <summary>
/// JSON deserializer with JSON Schema registry integration.
/// Reads Confluent wire format: [0x00][4-byte schema ID][JSON payload]
/// </summary>
/// <typeparam name="T">The type to deserialize to.</typeparam>
public sealed class SchemaRegistryJsonDeserializer<T> : IAsyncDeserializer<T>
{
    private readonly JsonSchemaSerializerConfig _config;
    private readonly ConcurrentDictionary<int, NJsonSchema.JsonSchema> _schemaCache = new();

    /// <summary>
    /// Create a new schema registry JSON deserializer.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    public SchemaRegistryJsonDeserializer(JsonSchemaSerializerConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <inheritdoc />
    public async ValueTask<T> DeserializeAsync(ReadOnlyMemory<byte> data, string topic, CancellationToken cancellationToken = default)
    {
        if (data.Length < SchemaRegistryWireFormat.HeaderSize)
            throw new ArgumentException($"Data too short. Expected at least {SchemaRegistryWireFormat.HeaderSize} bytes, got {data.Length}");

        // Read schema ID and convert payload to string before await
        var schemaId = SchemaRegistryWireFormat.ReadSchemaId(data.Span);
        var payload = data.Slice(SchemaRegistryWireFormat.HeaderSize);
        var json = Encoding.UTF8.GetString(payload.Span);

        // Get schema for validation
        var schema = await GetSchemaAsync(schemaId, cancellationToken);

        // Optionally validate against schema
        if (_config.ValidateOnDeserialize && schema != null)
        {
            var errors = schema.Validate(json);
            if (errors.Count > 0)
                throw new InvalidOperationException($"JSON validation failed: {string.Join(", ", errors.Select(e => e.ToString()))}");
        }

        var result = JsonSerializer.Deserialize<T>(json, _config.JsonOptions);
        if (result == null)
            throw new InvalidOperationException("Deserialization returned null");

        return result;
    }

    private async Task<NJsonSchema.JsonSchema?> GetSchemaAsync(int schemaId, CancellationToken cancellationToken)
    {
        if (_schemaCache.TryGetValue(schemaId, out var cached))
            return cached;

        var schemaInfo = await _config.SchemaRegistry.GetSchemaByIdAsync(schemaId, cancellationToken);
        if (schemaInfo == null)
            throw new InvalidOperationException($"Schema ID {schemaId} not found in registry");

        var schema = await NJsonSchema.JsonSchema.FromJsonAsync(schemaInfo.SchemaString, cancellationToken);
        _schemaCache[schemaId] = schema;

        return schema;
    }
}

using Kuestenlogik.Surgewave.Schema.Registry.Client;
using System.Collections.Concurrent;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Avro;

/// <summary>
/// Avro deserializer with schema registry integration.
/// Reads Confluent wire format: [0x00][4-byte schema ID][Avro payload]
/// </summary>
/// <remarks>
/// This implementation uses JSON as an intermediate format for Avro deserialization.
/// For high-performance binary Avro, consider using the Chr.Avro.Binary package directly.
/// </remarks>
/// <typeparam name="T">The type to deserialize to.</typeparam>
public sealed class SchemaRegistryAvroDeserializer<T> : IAsyncDeserializer<T>
{
    private readonly AvroSerializerConfig _config;
    private readonly ConcurrentDictionary<int, string> _schemaCache = new();
    private readonly Func<ReadOnlyMemory<byte>, T>? _customDeserializer;

    /// <summary>
    /// Create a new schema registry Avro deserializer using JSON decoding.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    public SchemaRegistryAvroDeserializer(AvroSerializerConfig config)
        : this(config, null)
    {
    }

    /// <summary>
    /// Create a new schema registry Avro deserializer with a custom binary deserializer.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    /// <param name="customDeserializer">Optional custom deserializer for binary Avro decoding.</param>
    public SchemaRegistryAvroDeserializer(AvroSerializerConfig config, Func<ReadOnlyMemory<byte>, T>? customDeserializer)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _customDeserializer = customDeserializer;
    }

    /// <inheritdoc />
    public async ValueTask<T> DeserializeAsync(ReadOnlyMemory<byte> data, string topic, CancellationToken cancellationToken = default)
    {
        if (data.Length < SchemaRegistryWireFormat.HeaderSize)
            throw new ArgumentException($"Data too short. Expected at least {SchemaRegistryWireFormat.HeaderSize} bytes, got {data.Length}");

        var span = data.Span;

        // Read schema ID from wire format header
        var schemaId = SchemaRegistryWireFormat.ReadSchemaId(span);

        // Validate schema exists in registry
        await ValidateSchemaAsync(schemaId, cancellationToken);

        // Get payload after header
        var payload = data.Slice(SchemaRegistryWireFormat.HeaderSize);

        // Deserialize
        if (_customDeserializer != null)
        {
            return _customDeserializer(payload);
        }

        // Use JSON decoding as fallback
        var result = JsonSerializer.Deserialize<T>(payload.Span);
        if (result == null)
            throw new InvalidOperationException("Deserialization returned null");

        return result;
    }

    private async Task ValidateSchemaAsync(int schemaId, CancellationToken cancellationToken)
    {
        if (_schemaCache.ContainsKey(schemaId))
            return;

        var schemaInfo = await _config.SchemaRegistry.GetSchemaByIdAsync(schemaId, cancellationToken);
        if (schemaInfo == null)
            throw new InvalidOperationException($"Schema ID {schemaId} not found in registry");

        _schemaCache[schemaId] = schemaInfo.SchemaString;
    }
}

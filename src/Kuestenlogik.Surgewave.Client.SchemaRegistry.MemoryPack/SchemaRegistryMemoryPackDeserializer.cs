using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.MemoryPack;

/// <summary>
/// MemoryPack deserializer with schema registry integration.
/// Reads Confluent wire format: [0x00 magic][4-byte schema ID][MemoryPack payload]
/// </summary>
/// <typeparam name="T">The type to deserialize to. Must have [MemoryPackable] attribute.</typeparam>
public sealed class SchemaRegistryMemoryPackDeserializer<T> : IAsyncDeserializer<T>
{
    private readonly MemoryPackSerializerConfig _config;
    private readonly ConcurrentDictionary<int, string> _schemaCache = new();

    /// <summary>
    /// Create a new schema registry MemoryPack deserializer.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    public SchemaRegistryMemoryPackDeserializer(MemoryPackSerializerConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
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

        // Deserialize with MemoryPack
        var result = global::MemoryPack.MemoryPackSerializer.Deserialize<T>(payload.Span);

        if (result == null)
            throw new InvalidOperationException("MemoryPack deserialization returned null");

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

using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Hyperion;

/// <summary>
/// Hyperion deserializer with schema registry integration.
/// Reads Confluent wire format: [0x00 magic][4-byte schema ID][Hyperion payload]
/// </summary>
/// <typeparam name="T">The type to deserialize to.</typeparam>
public sealed class SchemaRegistryHyperionDeserializer<T> : IAsyncDeserializer<T>
{
    private readonly HyperionSerializerConfig _config;
    private readonly ConcurrentDictionary<int, string> _schemaCache = new();
    private readonly global::Hyperion.Serializer _hyperionSerializer = new();

    /// <summary>
    /// Create a new schema registry Hyperion deserializer.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    public SchemaRegistryHyperionDeserializer(HyperionSerializerConfig config)
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

        // Deserialize with Hyperion
        using var stream = new MemoryStream(payload.ToArray());
        var result = _hyperionSerializer.Deserialize<T>(stream);

        if (result == null)
            throw new InvalidOperationException("Hyperion deserialization returned null");

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

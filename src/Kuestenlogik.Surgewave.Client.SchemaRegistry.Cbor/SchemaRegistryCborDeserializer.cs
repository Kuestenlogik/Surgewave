using System.Collections.Concurrent;
using Dahomey.Cbor;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Cbor;

/// <summary>
/// CBOR deserializer with schema registry integration.
/// Reads Confluent wire format: [0x00 magic][4-byte schema ID][CBOR payload]
/// </summary>
/// <typeparam name="T">The type to deserialize to.</typeparam>
public sealed class SchemaRegistryCborDeserializer<T> : IAsyncDeserializer<T>
{
    private readonly CborSerializerConfig _config;
    private readonly ConcurrentDictionary<int, string> _schemaCache = new();

    /// <summary>
    /// Create a new schema registry CBOR deserializer.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    public SchemaRegistryCborDeserializer(CborSerializerConfig config)
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
        var payload = data.Slice(SchemaRegistryWireFormat.HeaderSize).ToArray();

        // Deserialize with CBOR
        using var stream = new MemoryStream(payload);
        var result = await global::Dahomey.Cbor.Cbor.DeserializeAsync<T>(stream);

        if (result == null)
            throw new InvalidOperationException("CBOR deserialization returned null");

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

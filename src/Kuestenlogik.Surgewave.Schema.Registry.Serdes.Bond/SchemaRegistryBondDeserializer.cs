using Kuestenlogik.Surgewave.Schema.Registry.Client;
using System.Collections.Concurrent;
using global::Bond;
using global::Bond.IO.Unsafe;
using global::Bond.Protocols;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Bond;

/// <summary>
/// Bond deserializer with schema registry integration.
/// Reads Confluent wire format: [0x00 magic][4-byte schema ID][Bond CompactBinary payload]
/// </summary>
/// <typeparam name="T">The type to deserialize to. Must have Bond attributes.</typeparam>
public sealed class SchemaRegistryBondDeserializer<T> : IAsyncDeserializer<T>
{
    private readonly BondSerializerConfig _config;
    private readonly ConcurrentDictionary<int, string> _schemaCache = new();

    /// <summary>
    /// Create a new schema registry Bond deserializer.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    public SchemaRegistryBondDeserializer(BondSerializerConfig config)
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

        // Deserialize with Bond CompactBinary
        var input = new InputBuffer(payload);
        var reader = new CompactBinaryReader<InputBuffer>(input);
        var result = Deserialize<T>.From(reader);

        if (result == null)
            throw new InvalidOperationException("Bond deserialization returned null");

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

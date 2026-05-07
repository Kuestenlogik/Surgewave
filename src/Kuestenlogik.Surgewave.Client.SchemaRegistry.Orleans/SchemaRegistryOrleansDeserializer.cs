using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Client.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Orleans;

/// <summary>
/// Orleans deserializer with schema registry integration.
/// Reads Confluent wire format: [0x00 magic][4-byte schema ID][Orleans payload]
/// </summary>
/// <typeparam name="T">The type to deserialize to.</typeparam>
public sealed class SchemaRegistryOrleansDeserializer<T> : IAsyncDeserializer<T>
{
    private readonly OrleansSerializerConfig _config;
    private readonly ConcurrentDictionary<int, string> _schemaCache = new();
    private readonly Serializer _orleansSerializer;

    /// <summary>
    /// Create a new schema registry Orleans deserializer.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    public SchemaRegistryOrleansDeserializer(OrleansSerializerConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));

        var services = new ServiceCollection();
        services.AddSerializer();
        var provider = services.BuildServiceProvider();
        _orleansSerializer = provider.GetRequiredService<Serializer>();
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

        // Deserialize with Orleans
        var result = _orleansSerializer.Deserialize<T>(payload);

        if (result == null)
            throw new InvalidOperationException("Orleans deserialization returned null");

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

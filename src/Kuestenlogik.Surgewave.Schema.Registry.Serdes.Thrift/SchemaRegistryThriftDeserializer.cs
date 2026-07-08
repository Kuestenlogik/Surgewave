using Kuestenlogik.Surgewave.Schema.Registry.Client;
using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Client.Serialization;
using Thrift.Protocol;
using Thrift.Transport.Client;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Thrift;

/// <summary>
/// Thrift deserializer with schema registry integration.
/// Reads Confluent wire format: [0x00 magic][4-byte schema ID][Thrift CompactProtocol payload]
/// </summary>
/// <typeparam name="T">The type to deserialize to. Must implement TBase and have a parameterless constructor.</typeparam>
public sealed class SchemaRegistryThriftDeserializer<T> : IAsyncDeserializer<T> where T : global::Thrift.Protocol.TBase, new()
{
    private readonly ThriftSerializerConfig _config;
    private readonly ConcurrentDictionary<int, string> _schemaCache = new();

    /// <summary>
    /// Create a new schema registry Thrift deserializer.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    public SchemaRegistryThriftDeserializer(ThriftSerializerConfig config)
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

        // Deserialize with Thrift CompactProtocol
        using var stream = new MemoryStream(payload);
        using var transport = new TStreamTransport(stream, null, null);
        using var protocol = new TCompactProtocol(transport);
        var result = new T();
        await result.ReadAsync(protocol, cancellationToken);

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

using Kuestenlogik.Surgewave.Schema.Registry.Client;
using System.Collections.Concurrent;
using Google.Protobuf;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Protobuf;

/// <summary>
/// Protobuf deserializer with schema registry integration.
/// Reads Confluent wire format: [0x00][4-byte schema ID][varint message index][Protobuf payload]
/// </summary>
/// <typeparam name="T">The Protobuf message type to deserialize to.</typeparam>
public sealed class SchemaRegistryProtobufDeserializer<T> : IAsyncDeserializer<T> where T : IMessage<T>, new()
{
    private readonly ProtobufSerializerConfig _config;
    private readonly MessageParser<T> _parser;
    private readonly ConcurrentDictionary<int, bool> _schemaValidationCache = new();

    /// <summary>
    /// Create a new schema registry Protobuf deserializer.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    public SchemaRegistryProtobufDeserializer(ProtobufSerializerConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _parser = new MessageParser<T>(() => new T());
    }

    /// <inheritdoc />
    public async ValueTask<T> DeserializeAsync(ReadOnlyMemory<byte> data, string topic, CancellationToken cancellationToken = default)
    {
        if (data.Length < SchemaRegistryWireFormat.HeaderSize)
            throw new ArgumentException($"Data too short. Expected at least {SchemaRegistryWireFormat.HeaderSize} bytes, got {data.Length}");

        // Read schema ID from wire format header (before await)
        var schemaId = SchemaRegistryWireFormat.ReadSchemaId(data.Span);

        // Calculate payload offset before await
        var offset = SchemaRegistryWireFormat.HeaderSize;
        offset = SkipVarint(data.Span, offset);

        // Validate schema exists (for logging/debugging)
        await ValidateSchemaExistsAsync(schemaId, cancellationToken);

        // Parse Protobuf payload
        var payload = data.Slice(offset);
        return _parser.ParseFrom(payload.Span);
    }

    private async Task ValidateSchemaExistsAsync(int schemaId, CancellationToken cancellationToken)
    {
        if (_schemaValidationCache.ContainsKey(schemaId))
            return;

        var schemaInfo = await _config.SchemaRegistry.GetSchemaByIdAsync(schemaId, cancellationToken);
        if (schemaInfo == null)
            throw new InvalidOperationException($"Schema ID {schemaId} not found in registry");

        _schemaValidationCache[schemaId] = true;
    }

    private static int SkipVarint(ReadOnlySpan<byte> span, int offset)
    {
        while (offset < span.Length)
        {
            var b = span[offset++];

            if ((b & 0x80) == 0)
                return offset;
        }

        throw new InvalidOperationException("Incomplete varint");
    }
}

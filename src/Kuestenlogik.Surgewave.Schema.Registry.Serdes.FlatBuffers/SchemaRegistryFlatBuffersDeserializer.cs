using Kuestenlogik.Surgewave.Schema.Registry.Client;
using System.Collections.Concurrent;
using Google.FlatBuffers;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.FlatBuffers;

/// <summary>
/// FlatBuffers deserializer with schema registry integration.
/// Reads Confluent wire format: [0x00][4-byte schema ID][FlatBuffers payload]
/// </summary>
/// <typeparam name="T">The FlatBuffers table type to deserialize to.</typeparam>
public sealed class SchemaRegistryFlatBuffersDeserializer<T> : IAsyncDeserializer<T> where T : struct, IFlatbufferObject
{
    private readonly FlatBuffersSerializerConfig _config;
    private readonly Func<ByteBuffer, T> _deserializeFunc;
    private readonly ConcurrentDictionary<int, bool> _schemaValidationCache = new();

    /// <summary>
    /// Create a new schema registry FlatBuffers deserializer.
    /// </summary>
    /// <param name="config">Deserializer configuration.</param>
    /// <param name="deserializeFunc">Function to deserialize bytes to a FlatBuffer object.</param>
    public SchemaRegistryFlatBuffersDeserializer(FlatBuffersSerializerConfig config, Func<ByteBuffer, T> deserializeFunc)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _deserializeFunc = deserializeFunc ?? throw new ArgumentNullException(nameof(deserializeFunc));
    }

    /// <inheritdoc />
    public async ValueTask<T> DeserializeAsync(ReadOnlyMemory<byte> data, string topic, CancellationToken cancellationToken = default)
    {
        if (data.Length < SchemaRegistryWireFormat.HeaderSize)
            throw new ArgumentException($"Data too short. Expected at least {SchemaRegistryWireFormat.HeaderSize} bytes, got {data.Length}");

        var span = data.Span;

        // Read schema ID from wire format header
        var schemaId = SchemaRegistryWireFormat.ReadSchemaId(span);

        // Validate schema exists
        await ValidateSchemaExistsAsync(schemaId, cancellationToken);

        // Get payload after header
        var payload = data.Slice(SchemaRegistryWireFormat.HeaderSize);

        // Create ByteBuffer for FlatBuffers
        var byteBuffer = new ByteBuffer(payload.ToArray());

        // Deserialize
        return _deserializeFunc(byteBuffer);
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
}

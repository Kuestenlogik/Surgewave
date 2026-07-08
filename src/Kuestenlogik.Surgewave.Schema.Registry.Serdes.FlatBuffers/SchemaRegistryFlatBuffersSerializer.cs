using Kuestenlogik.Surgewave.Schema.Registry.Client;
using System.Collections.Concurrent;
using Google.FlatBuffers;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.FlatBuffers;

/// <summary>
/// FlatBuffers serializer with schema registry integration.
/// Uses Confluent wire format: [0x00][4-byte schema ID][FlatBuffers payload]
/// </summary>
/// <remarks>
/// This serializer works with types that implement IFlatbufferObject and have a
/// static SerializeToBytes method or can provide their byte representation.
/// </remarks>
/// <typeparam name="T">The FlatBuffers table type to serialize.</typeparam>
public sealed class SchemaRegistryFlatBuffersSerializer<T> : IAsyncSerializer<T> where T : struct, IFlatbufferObject
{
    private readonly FlatBuffersSerializerConfig _config;
    private readonly ConcurrentDictionary<string, int> _schemaIdCache = new();
    private readonly Func<T, byte[]> _serializeFunc;

    /// <summary>
    /// Create a new schema registry FlatBuffers serializer.
    /// </summary>
    /// <param name="config">Serializer configuration.</param>
    /// <param name="serializeFunc">Function to serialize the FlatBuffer object to bytes.</param>
    public SchemaRegistryFlatBuffersSerializer(FlatBuffersSerializerConfig config, Func<T, byte[]> serializeFunc)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _serializeFunc = serializeFunc ?? throw new ArgumentNullException(nameof(serializeFunc));
    }

    /// <inheritdoc />
    public async ValueTask<byte[]?> SerializeAsync(T data, string topic, CancellationToken cancellationToken = default)
    {
        if (EqualityComparer<T>.Default.Equals(data, default))
            return null;

        // Get or register schema
        var schemaId = await GetOrRegisterSchemaAsync(topic, cancellationToken);

        // Serialize the FlatBuffer
        var flatBufferBytes = _serializeFunc(data);

        // Build result with wire format header
        var result = new byte[SchemaRegistryWireFormat.HeaderSize + flatBufferBytes.Length];
        SchemaRegistryWireFormat.WriteHeader(result, schemaId);
        flatBufferBytes.CopyTo(result.AsSpan(SchemaRegistryWireFormat.HeaderSize));

        return result;
    }

    private async Task<int> GetOrRegisterSchemaAsync(string topic, CancellationToken cancellationToken)
    {
        var recordName = typeof(T).FullName;
        var subject = _config.SubjectNameStrategy.GetSubjectName(topic, _config.IsKey, recordName);

        if (_schemaIdCache.TryGetValue(subject, out var cachedId))
            return cachedId;

        if (_config.AutoRegisterSchemas)
        {
            if (string.IsNullOrEmpty(_config.SchemaString))
                throw new InvalidOperationException("SchemaString is required for auto-registration with FlatBuffers");

            var result = await _config.SchemaRegistry.RegisterSchemaAsync(subject, _config.SchemaString, "FLATBUFFERS", cancellationToken);
            _schemaIdCache[subject] = result.SchemaId;
            return result.SchemaId;
        }

        // Look up existing schema
        var versions = await _config.SchemaRegistry.GetSubjectVersionsAsync(subject, cancellationToken);
        if (versions.Count == 0)
            throw new InvalidOperationException($"No schema registered for subject '{subject}' and auto-registration is disabled");

        var latestVersion = versions.Max();
        var schemaInfo = await _config.SchemaRegistry.GetSchemaByVersionAsync(subject, latestVersion, cancellationToken);
        if (schemaInfo == null)
            throw new InvalidOperationException($"Schema not found for subject '{subject}' version {latestVersion}");

        _schemaIdCache[subject] = schemaInfo.Id;
        return schemaInfo.Id;
    }
}

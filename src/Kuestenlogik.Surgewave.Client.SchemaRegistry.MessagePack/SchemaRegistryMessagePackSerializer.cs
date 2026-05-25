using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.MessagePack;

/// <summary>
/// MessagePack serializer with schema registry integration.
/// Uses Confluent wire format: [0x00 magic][4-byte schema ID][MessagePack payload]
/// </summary>
/// <typeparam name="T">The type to serialize.</typeparam>
public sealed class SchemaRegistryMessagePackSerializer<T> : IAsyncSerializer<T>
{
    private readonly MessagePackSerializerConfig _config;
    private readonly ConcurrentDictionary<string, int> _schemaIdCache = new();

    /// <summary>
    /// Create a new schema registry MessagePack serializer.
    /// </summary>
    /// <param name="config">Serializer configuration.</param>
    public SchemaRegistryMessagePackSerializer(MessagePackSerializerConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <inheritdoc />
    public async ValueTask<byte[]?> SerializeAsync(T? data, string topic, CancellationToken cancellationToken = default)
    {
        if (data == null)
            return null;

        // Get or register schema
        var schemaId = await GetOrRegisterSchemaAsync(topic, cancellationToken);

        // Serialize the data with MessagePack
        var payload = global::MessagePack.MessagePackSerializer.Serialize(data, cancellationToken: cancellationToken);

        // Build result with wire format header
        var result = new byte[SchemaRegistryWireFormat.HeaderSize + payload.Length];
        SchemaRegistryWireFormat.WriteHeader(result, schemaId);
        payload.CopyTo(result.AsSpan(SchemaRegistryWireFormat.HeaderSize));

        return result;
    }

    private async Task<int> GetOrRegisterSchemaAsync(string topic, CancellationToken cancellationToken)
    {
        var subject = _config.SubjectNameStrategy.GetSubjectName(topic, _config.IsKey, typeof(T).FullName);

        if (_schemaIdCache.TryGetValue(subject, out var cachedId))
            return cachedId;

        var schemaString = typeof(T).AssemblyQualifiedName ?? typeof(T).FullName ?? typeof(T).Name;

        if (_config.AutoRegisterSchemas)
        {
            var result = await _config.SchemaRegistry.RegisterSchemaAsync(subject, schemaString, "MSGPACK", cancellationToken);
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

using Kuestenlogik.Surgewave.Schema.Registry.Client;
using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Client.Serialization;
using Thrift.Protocol;
using Thrift.Transport.Client;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Thrift;

/// <summary>
/// Thrift serializer with schema registry integration.
/// Uses Confluent wire format: [0x00 magic][4-byte schema ID][Thrift CompactProtocol payload]
/// </summary>
/// <typeparam name="T">The type to serialize. Must implement TBase.</typeparam>
public sealed class SchemaRegistryThriftSerializer<T> : IAsyncSerializer<T> where T : global::Thrift.Protocol.TBase
{
    private readonly ThriftSerializerConfig _config;
    private readonly ConcurrentDictionary<string, int> _schemaIdCache = new();

    /// <summary>
    /// Create a new schema registry Thrift serializer.
    /// </summary>
    /// <param name="config">Serializer configuration.</param>
    public SchemaRegistryThriftSerializer(ThriftSerializerConfig config)
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

        // Serialize the data with Thrift CompactProtocol
        using var stream = new MemoryStream();
        using var transport = new TStreamTransport(null, stream, null);
        using var protocol = new TCompactProtocol(transport);
        await data.WriteAsync(protocol, cancellationToken);
        await transport.FlushAsync(cancellationToken);
        var payload = stream.ToArray();

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
            var result = await _config.SchemaRegistry.RegisterSchemaAsync(subject, schemaString, "THRIFT", cancellationToken);
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

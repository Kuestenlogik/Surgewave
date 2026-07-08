using Kuestenlogik.Surgewave.Schema.Registry.Client;
using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Client.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Orleans;

/// <summary>
/// Orleans serializer with schema registry integration.
/// Uses Confluent wire format: [0x00 magic][4-byte schema ID][Orleans payload]
/// </summary>
/// <typeparam name="T">The type to serialize.</typeparam>
public sealed class SchemaRegistryOrleansSerializer<T> : IAsyncSerializer<T>
{
    private readonly OrleansSerializerConfig _config;
    private readonly ConcurrentDictionary<string, int> _schemaIdCache = new();
    private readonly Serializer _orleansSerializer;

    /// <summary>
    /// Create a new schema registry Orleans serializer.
    /// </summary>
    /// <param name="config">Serializer configuration.</param>
    public SchemaRegistryOrleansSerializer(OrleansSerializerConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));

        var services = new ServiceCollection();
        services.AddSerializer();
        var provider = services.BuildServiceProvider();
        _orleansSerializer = provider.GetRequiredService<Serializer>();
    }

    /// <inheritdoc />
    public async ValueTask<byte[]?> SerializeAsync(T? data, string topic, CancellationToken cancellationToken = default)
    {
        if (data == null)
            return null;

        // Get or register schema
        var schemaId = await GetOrRegisterSchemaAsync(topic, cancellationToken);

        // Serialize the data with Orleans
        var payload = _orleansSerializer.SerializeToArray(data);

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
            var result = await _config.SchemaRegistry.RegisterSchemaAsync(subject, schemaString, "ORLEANS", cancellationToken);
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

using Kuestenlogik.Surgewave.Schema.Registry.Client;
using System.Collections.Concurrent;
using global::Bond;
using global::Bond.IO.Unsafe;
using global::Bond.Protocols;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Bond;

/// <summary>
/// Bond serializer with schema registry integration.
/// Uses Confluent wire format: [0x00 magic][4-byte schema ID][Bond CompactBinary payload]
/// </summary>
/// <typeparam name="T">The type to serialize. Must have Bond attributes.</typeparam>
public sealed class SchemaRegistryBondSerializer<T> : IAsyncSerializer<T>
{
    private readonly BondSerializerConfig _config;
    private readonly ConcurrentDictionary<string, int> _schemaIdCache = new();

    /// <summary>
    /// Create a new schema registry Bond serializer.
    /// </summary>
    /// <param name="config">Serializer configuration.</param>
    public SchemaRegistryBondSerializer(BondSerializerConfig config)
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

        // Serialize the data with Bond CompactBinary
        var output = new OutputBuffer();
        var writer = new CompactBinaryWriter<OutputBuffer>(output);
        Serialize.To(writer, data);
        var payload = output.Data;

        // Build result with wire format header
        var result = new byte[SchemaRegistryWireFormat.HeaderSize + payload.Count];
        SchemaRegistryWireFormat.WriteHeader(result, schemaId);
        payload.Array.AsSpan(payload.Offset, payload.Count).CopyTo(result.AsSpan(SchemaRegistryWireFormat.HeaderSize));

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
            var result = await _config.SchemaRegistry.RegisterSchemaAsync(subject, schemaString, "BOND", cancellationToken);
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

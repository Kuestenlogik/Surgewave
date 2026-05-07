using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.FlatBuffers;

/// <summary>
/// Configuration for FlatBuffers schema registry serializers.
/// </summary>
public sealed class FlatBuffersSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new FlatBuffers serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public FlatBuffersSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }

    /// <summary>
    /// The FlatBuffers schema (IDL) string. Required for auto-registration.
    /// </summary>
    public string? SchemaString { get; init; }
}

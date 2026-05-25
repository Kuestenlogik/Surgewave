using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.MemoryPack;

/// <summary>
/// Configuration for MemoryPack schema registry serializers.
/// </summary>
public sealed class MemoryPackSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new MemoryPack serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public MemoryPackSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }
}

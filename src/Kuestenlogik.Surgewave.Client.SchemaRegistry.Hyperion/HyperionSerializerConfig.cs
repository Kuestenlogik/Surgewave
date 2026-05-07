using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Hyperion;

/// <summary>
/// Configuration for Hyperion schema registry serializers.
/// </summary>
public sealed class HyperionSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new Hyperion serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public HyperionSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }
}

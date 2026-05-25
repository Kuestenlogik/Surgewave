using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Orleans;

/// <summary>
/// Configuration for Orleans schema registry serializers.
/// </summary>
public sealed class OrleansSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new Orleans serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public OrleansSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }
}

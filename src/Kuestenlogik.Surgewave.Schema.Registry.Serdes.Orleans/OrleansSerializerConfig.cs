using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Orleans;

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

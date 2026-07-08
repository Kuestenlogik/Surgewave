using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Bond;

/// <summary>
/// Configuration for Bond schema registry serializers.
/// </summary>
public sealed class BondSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new Bond serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public BondSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }
}

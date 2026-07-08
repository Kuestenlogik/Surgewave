using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.CapnProto;

/// <summary>
/// Configuration for Cap'n Proto schema registry serializers.
/// </summary>
public sealed class CapnProtoSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new Cap'n Proto serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public CapnProtoSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }
}

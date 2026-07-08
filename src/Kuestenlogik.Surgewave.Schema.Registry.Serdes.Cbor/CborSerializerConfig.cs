using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Cbor;

/// <summary>
/// Configuration for CBOR schema registry serializers.
/// </summary>
public sealed class CborSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new CBOR serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public CborSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }
}

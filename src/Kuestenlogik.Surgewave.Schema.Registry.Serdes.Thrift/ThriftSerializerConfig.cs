using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Thrift;

/// <summary>
/// Configuration for Thrift schema registry serializers.
/// </summary>
public sealed class ThriftSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new Thrift serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public ThriftSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }
}

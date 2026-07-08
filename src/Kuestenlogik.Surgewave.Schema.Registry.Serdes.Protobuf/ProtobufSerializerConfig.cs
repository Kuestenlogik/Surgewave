using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Protobuf;

/// <summary>
/// Configuration for Protobuf schema registry serializers.
/// </summary>
public sealed class ProtobufSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new Protobuf serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public ProtobufSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }
}

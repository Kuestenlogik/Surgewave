using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Avro;

/// <summary>
/// Configuration for Avro schema registry serializers.
/// </summary>
public sealed class AvroSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new Avro serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public AvroSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }
}

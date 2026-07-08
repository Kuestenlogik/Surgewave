using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.MessagePack;

/// <summary>
/// Configuration for MessagePack schema registry serializers.
/// </summary>
public sealed class MessagePackSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new MessagePack serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public MessagePackSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }
}

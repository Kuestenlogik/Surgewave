using Kuestenlogik.Surgewave.Schema.Registry.Client;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Json;

/// <summary>
/// Configuration for JSON Schema registry serializers.
/// </summary>
public sealed class JsonSchemaSerializerConfig : SchemaRegistrySerializerConfig
{
    /// <summary>
    /// Create a new JSON Schema serializer configuration.
    /// </summary>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    public JsonSchemaSerializerConfig(ISchemaRegistryOperations schemaRegistry)
    {
        SchemaRegistry = schemaRegistry;
    }

    /// <summary>
    /// JSON serializer options. Defaults to camelCase property naming.
    /// </summary>
    public JsonSerializerOptions JsonOptions { get; set; } = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    /// <summary>
    /// Whether to validate JSON against the schema before serialization. Defaults to false.
    /// </summary>
    public bool ValidateOnSerialize { get; set; }

    /// <summary>
    /// Whether to validate JSON against the schema after deserialization. Defaults to false.
    /// </summary>
    public bool ValidateOnDeserialize { get; set; }
}

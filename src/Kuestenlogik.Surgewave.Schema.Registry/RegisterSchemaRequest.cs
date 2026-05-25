using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Request to register a schema.
/// </summary>
public sealed class RegisterSchemaRequest
{
    /// <summary>
    /// The schema string.
    /// </summary>
    [JsonPropertyName("schema")]
    public string? Schema { get; set; }

    /// <summary>
    /// Schema type (AVRO, JSON, PROTOBUF).
    /// </summary>
    [JsonPropertyName("schemaType")]
    public string? SchemaType { get; set; }

    /// <summary>
    /// References to other schemas.
    /// </summary>
    [JsonPropertyName("references")]
    public List<SchemaReferenceRequest>? References { get; set; }
}

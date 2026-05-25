using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Response when getting schema by ID.
/// </summary>
public sealed class GetSchemaByIdResponse
{
    [JsonPropertyName("schema")]
    public string Schema { get; set; } = "";

    [JsonPropertyName("schemaType")]
    public string SchemaType { get; set; } = "AVRO";

    [JsonPropertyName("references")]
    public List<SchemaReferenceResponse>? References { get; set; }
}

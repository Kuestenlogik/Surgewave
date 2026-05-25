using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Response after registering a schema.
/// </summary>
public sealed class RegisterSchemaResponse
{
    [JsonPropertyName("id")]
    public int Id { get; set; }
}

using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Subject-version pair.
/// </summary>
public sealed class SubjectVersion
{
    [JsonPropertyName("subject")]
    public string Subject { get; set; } = "";

    [JsonPropertyName("version")]
    public int Version { get; set; }
}

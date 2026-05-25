using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Configuration request.
/// </summary>
public sealed class ConfigRequest
{
    [JsonPropertyName("compatibility")]
    public string? Compatibility { get; set; }
}

using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Configuration response.
/// </summary>
public sealed class ConfigResponse
{
    [JsonPropertyName("compatibilityLevel")]
    public string CompatibilityLevel { get; set; } = "BACKWARD";
}

using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Compatibility check response.
/// </summary>
public sealed class CompatibilityCheckResponse
{
    [JsonPropertyName("is_compatible")]
    public bool IsCompatible { get; set; }

    [JsonPropertyName("messages")]
    public IReadOnlyList<string>? Messages { get; set; }
}

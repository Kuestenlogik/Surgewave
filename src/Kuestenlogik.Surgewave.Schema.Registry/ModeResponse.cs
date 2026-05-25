using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Mode response.
/// </summary>
public sealed class ModeResponse
{
    [JsonPropertyName("mode")]
    public string Mode { get; set; } = "READWRITE";
}

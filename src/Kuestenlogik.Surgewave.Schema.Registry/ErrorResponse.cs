using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Error response.
/// </summary>
public sealed class ErrorResponse
{
    [JsonPropertyName("error_code")]
    public int ErrorCode { get; set; }

    [JsonPropertyName("message")]
    public string Message { get; set; } = "";
}

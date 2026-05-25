using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket;

/// <summary>
/// JSON error message sent to WebSocket clients when an operation fails.
/// </summary>
public sealed class WebSocketErrorMessage
{
    /// <summary>
    /// Error type (e.g., "invalid_request", "topic_not_found", "internal_error").
    /// </summary>
    [JsonPropertyName("error")]
    public required string Error { get; set; }

    /// <summary>
    /// Human-readable error message.
    /// </summary>
    [JsonPropertyName("message")]
    public required string Message { get; set; }

    /// <summary>
    /// Optional correlation ID if the client sent one.
    /// </summary>
    [JsonPropertyName("correlationId")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? CorrelationId { get; set; }
}

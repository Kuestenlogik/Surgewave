using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;

/// <summary>
/// Base WebSocket message structure.
/// </summary>
public class WebSocketMessage
{
    /// <summary>
    /// Message type identifier.
    /// </summary>
    [JsonPropertyName("type")]
    public required string Type { get; set; }

    /// <summary>
    /// Optional correlation ID for request/response matching.
    /// </summary>
    [JsonPropertyName("id")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Id { get; set; }

    /// <summary>
    /// Optional cluster ID. Uses default cluster if not specified.
    /// </summary>
    [JsonPropertyName("cluster_id")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ClusterId { get; set; }
}

/// <summary>
/// WebSocket message with typed payload.
/// </summary>
/// <typeparam name="T">Payload type.</typeparam>
public class WebSocketMessage<T> : WebSocketMessage where T : class
{
    /// <summary>
    /// Message payload.
    /// </summary>
    [JsonPropertyName("payload")]
    public T? Payload { get; set; }
}

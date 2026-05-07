using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket;

/// <summary>
/// JSON message sent by clients to produce messages to a Surgewave topic via WebSocket.
/// </summary>
public sealed class WebSocketProduceMessage
{
    /// <summary>
    /// Optional message key for partitioning.
    /// </summary>
    [JsonPropertyName("key")]
    public string? Key { get; set; }

    /// <summary>
    /// Message value (the actual payload).
    /// </summary>
    [JsonPropertyName("value")]
    public object? Value { get; set; }

    /// <summary>
    /// Optional message headers.
    /// </summary>
    [JsonPropertyName("headers")]
    public Dictionary<string, string>? Headers { get; set; }
}

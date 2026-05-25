using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket;

/// <summary>
/// JSON message sent by clients to subscribe to multiple Surgewave topics via a single WebSocket connection.
/// </summary>
public sealed class WebSocketSubscribeMessage
{
    /// <summary>
    /// Action to perform: "subscribe" or "unsubscribe".
    /// </summary>
    [JsonPropertyName("action")]
    public required string Action { get; set; }

    /// <summary>
    /// List of topic names to subscribe to or unsubscribe from.
    /// </summary>
    [JsonPropertyName("topics")]
    public required List<string> Topics { get; set; }

    /// <summary>
    /// Optional: starting offset for each topic. Default: latest (end of topic).
    /// Use -1 for latest, -2 for earliest.
    /// </summary>
    [JsonPropertyName("offsets")]
    public Dictionary<string, long>? Offsets { get; set; }
}

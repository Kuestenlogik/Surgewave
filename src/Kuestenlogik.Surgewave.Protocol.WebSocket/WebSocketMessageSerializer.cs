using System.Text.Json;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket;

/// <summary>
/// Serializes and deserializes WebSocket protocol messages using source-generated JSON contexts
/// for optimal performance and AOT compatibility.
/// </summary>
public static class WebSocketMessageSerializer
{
    /// <summary>
    /// Deserialize a produce message from JSON bytes.
    /// </summary>
    public static WebSocketProduceMessage? DeserializeProduceMessage(ReadOnlySpan<byte> json)
        => JsonSerializer.Deserialize(json, WebSocketJsonContext.Default.WebSocketProduceMessage);

    /// <summary>
    /// Deserialize a subscribe message from JSON bytes.
    /// </summary>
    public static WebSocketSubscribeMessage? DeserializeSubscribeMessage(ReadOnlySpan<byte> json)
        => JsonSerializer.Deserialize(json, WebSocketJsonContext.Default.WebSocketSubscribeMessage);

    /// <summary>
    /// Serialize a consume message to JSON bytes.
    /// </summary>
    public static byte[] SerializeConsumeMessage(WebSocketConsumeMessage message)
        => JsonSerializer.SerializeToUtf8Bytes(message, WebSocketJsonContext.Default.WebSocketConsumeMessage);

    /// <summary>
    /// Serialize an error message to JSON bytes.
    /// </summary>
    public static byte[] SerializeErrorMessage(WebSocketErrorMessage error)
        => JsonSerializer.SerializeToUtf8Bytes(error, WebSocketJsonContext.Default.WebSocketErrorMessage);
}

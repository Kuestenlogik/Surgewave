using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket;

/// <summary>
/// JSON source generator context for WebSocket protocol message types.
/// Enables trimming and AOT compilation by avoiding reflection-based serialization.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    WriteIndented = false,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(WebSocketProduceMessage))]
[JsonSerializable(typeof(WebSocketConsumeMessage))]
[JsonSerializable(typeof(WebSocketSubscribeMessage))]
[JsonSerializable(typeof(WebSocketErrorMessage))]
[JsonSerializable(typeof(Dictionary<string, string>))]
[JsonSerializable(typeof(List<string>))]
[JsonSerializable(typeof(Dictionary<string, long>))]
internal sealed partial class WebSocketJsonContext : JsonSerializerContext;

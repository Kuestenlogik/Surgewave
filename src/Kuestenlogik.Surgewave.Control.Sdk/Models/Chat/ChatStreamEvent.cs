namespace Kuestenlogik.Surgewave.Control.Models.Chat;

/// <summary>
/// Represents a single event received from the streaming chat SSE endpoint.
/// </summary>
public sealed class ChatStreamEvent
{
    /// <summary>
    /// Event type: "token", "done", or "error".
    /// </summary>
    public string Type { get; set; } = "";

    /// <summary>
    /// Partial token text (for "token" events).
    /// </summary>
    public string? Token { get; set; }

    /// <summary>
    /// Session identifier (for "done" events).
    /// </summary>
    public string? SessionId { get; set; }

    /// <summary>
    /// Message identifier (for "done" events).
    /// </summary>
    public string? MessageId { get; set; }

    /// <summary>
    /// Complete response content (for "done" events).
    /// </summary>
    public string? FullContent { get; set; }

    /// <summary>
    /// Error description (for "error" events).
    /// </summary>
    public string? Error { get; set; }
}

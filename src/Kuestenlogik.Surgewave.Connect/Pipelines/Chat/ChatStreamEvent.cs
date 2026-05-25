namespace Kuestenlogik.Surgewave.Connect.Pipelines.Chat;

/// <summary>
/// Represents a single event in a streaming chat response.
/// Sent via Server-Sent Events (SSE) to the client.
/// </summary>
public sealed record ChatStreamEvent
{
    /// <summary>
    /// Event type: "token", "done", or "error".
    /// </summary>
    public required string Type { get; init; }

    /// <summary>
    /// Partial token text (for "token" events).
    /// </summary>
    public string? Token { get; init; }

    /// <summary>
    /// Session identifier (for "done" events).
    /// </summary>
    public string? SessionId { get; init; }

    /// <summary>
    /// Message identifier (for "done" events).
    /// </summary>
    public string? MessageId { get; init; }

    /// <summary>
    /// Complete response content (for "done" events).
    /// </summary>
    public string? FullContent { get; init; }

    /// <summary>
    /// Error description (for "error" events).
    /// </summary>
    public string? Error { get; init; }
}

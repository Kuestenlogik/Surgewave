namespace Kuestenlogik.Surgewave.Connect.Pipelines.Chat;

/// <summary>
/// A single message in a pipeline chat session.
/// </summary>
public sealed record ChatMessage
{
    public required string Id { get; init; }
    public required string Role { get; init; } // "user", "assistant", "system"
    public required string Content { get; init; }
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
    public Dictionary<string, string>? Metadata { get; init; }
}

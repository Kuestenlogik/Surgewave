namespace Kuestenlogik.Surgewave.Control.Models.Chat;

public record ChatMessageModel
{
    public required string Id { get; init; }
    public required string Role { get; init; }
    public required string Content { get; set; }
    public DateTimeOffset Timestamp { get; init; }
    public Dictionary<string, string>? Metadata { get; init; }
}

public record ChatRequest
{
    public required string Message { get; init; }
    public string? SessionId { get; init; }
}

public record ChatResponse
{
    public required string SessionId { get; init; }
    public required string MessageId { get; init; }
    public required string Content { get; init; }
    public required string Role { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public Dictionary<string, string>? Metadata { get; init; }
}

public record AsyncChatResponse
{
    public required string SessionId { get; init; }
    public required string MessageId { get; init; }
}

public record ChatHistoryResponse
{
    public required string SessionId { get; init; }
    public required string PipelineId { get; init; }
    public IReadOnlyList<ChatMessageModel> Messages { get; init; } = [];
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset LastActivityAt { get; init; }
}

public record ChatSessionListResponse
{
    public required string PipelineId { get; init; }
    public IReadOnlyList<ChatSessionSummary> Sessions { get; init; } = [];
}

public record ChatSessionSummary
{
    public required string SessionId { get; init; }
    public int MessageCount { get; init; }
    public int PendingCount { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset LastActivityAt { get; init; }
}

public record ChatTopicInfo
{
    public required string PipelineId { get; init; }
    public required string SignalTopic { get; init; }
    public required string ResponseTopic { get; init; }
}

namespace Kuestenlogik.Surgewave.Control.Models.Collaboration;

public sealed class PipelineComment
{
    public string Id { get; init; } = Guid.NewGuid().ToString("N");
    public required string PipelineId { get; init; }
    public string? NodeId { get; init; }
    public string? ConnectionId { get; init; }
    public required string Author { get; init; }
    public required string Text { get; set; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public bool Resolved { get; set; }
}

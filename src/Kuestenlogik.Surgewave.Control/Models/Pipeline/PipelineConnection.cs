namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

public record PipelineConnection
{
    public required string Id { get; init; }
    public required string SourceNodeId { get; init; }
    public required string TargetNodeId { get; init; }
    public string? InternalTopic { get; init; }

    /// <summary>
    /// Connection type: "normal" (default) or "error" for error-routing connections.
    /// </summary>
    public string? Type { get; init; }
}

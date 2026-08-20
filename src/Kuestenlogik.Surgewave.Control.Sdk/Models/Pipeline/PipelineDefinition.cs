namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

public record PipelineDefinition
{
    public required string Id { get; init; }
    public required string Name { get; init; }
    public string? Description { get; init; }
    public required List<PipelineNode> Nodes { get; init; }
    public required List<PipelineConnection> Connections { get; init; }
    public PipelineStatus Status { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? UpdatedAt { get; init; }
    /// <summary>
    /// Server-side parameters are a flat key→value map (serialized as a JSON object).
    /// The richer editor-side <see cref="PipelineParameter"/> list is client-only.
    /// </summary>
    public Dictionary<string, string>? Parameters { get; init; }

    public List<PipelineEnvironment> Environments { get; init; } = [];
    public ScheduleConfig? Schedule { get; init; }
}

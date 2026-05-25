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
    public List<PipelineParameter> Parameters { get; init; } = [];
    public List<PipelineEnvironment> Environments { get; init; } = [];
    public ScheduleConfig? Schedule { get; init; }
}

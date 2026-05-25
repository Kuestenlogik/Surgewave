namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Context for resolving pipeline variables during configuration.
/// </summary>
public sealed record PipelineVariableContext
{
    public required string PipelineId { get; init; }
    public required string PipelineName { get; init; }
    public string? NodeId { get; init; }
    public IReadOnlyDictionary<string, string> Parameters { get; init; } = new Dictionary<string, string>();
}

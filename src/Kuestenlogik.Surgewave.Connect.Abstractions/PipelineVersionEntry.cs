namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// A versioned snapshot of a pipeline definition.
/// </summary>
public record PipelineVersionEntry
{
    public required int Version { get; init; }
    public required PipelineDefinition Definition { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public string? ChangeDescription { get; init; }
}

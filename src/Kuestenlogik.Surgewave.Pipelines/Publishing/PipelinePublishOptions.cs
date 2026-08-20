namespace Kuestenlogik.Surgewave.Pipelines.Publishing;

/// <summary>Options for publishing a pipeline to a broker.</summary>
public sealed record PipelinePublishOptions
{
    /// <summary>How to handle an existing pipeline with the same name.</summary>
    public PublishMode Mode { get; init; } = PublishMode.CreateNew;

    /// <summary>Deploys the pipeline under this name instead of its built-in one.</summary>
    public string? NameOverride { get; init; }

    /// <summary>Starts the pipeline after publishing.</summary>
    public bool Start { get; init; }
}

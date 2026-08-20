namespace Kuestenlogik.Surgewave.Pipelines.Publishing;

/// <summary>Outcome of publishing one pipeline.</summary>
public sealed record PublishResult
{
    /// <summary>The broker-assigned pipeline id.</summary>
    public required string PipelineId { get; init; }

    /// <summary>The name the pipeline was deployed under.</summary>
    public required string Name { get; init; }

    /// <summary>True when an existing pipeline was updated instead of a new one created.</summary>
    public bool Replaced { get; init; }

    /// <summary>True when the replaced pipeline was running and had to be stopped for the update.</summary>
    public bool WasRunning { get; init; }

    /// <summary>True when the pipeline was started after publishing.</summary>
    public bool Started { get; init; }
}

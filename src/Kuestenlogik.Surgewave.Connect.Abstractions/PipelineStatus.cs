namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Execution status of a <see cref="PipelineDefinition"/>.
/// </summary>
public enum PipelineStatus
{
    /// <summary>The pipeline is configured but has not been started.</summary>
    Draft,
    /// <summary>The pipeline is actively processing data.</summary>
    Running,
    /// <summary>The pipeline has been stopped by the user.</summary>
    Stopped,
    /// <summary>The pipeline encountered an error and is no longer processing.</summary>
    Failed
}

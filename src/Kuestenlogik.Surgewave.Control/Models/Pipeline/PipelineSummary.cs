namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Lightweight summary of a pipeline for use in the node palette.
/// </summary>
public record PipelineSummary(string Id, string Name, string? Description, int NodeCount);

namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// A configurable parameter for a pipeline.
/// Supports variable substitution via ${param.name} syntax in node configs.
/// </summary>
public record PipelineParameter
{
    public required string Name { get; init; }
    public string DefaultValue { get; init; } = "";
    public string? Description { get; init; }
    public bool IsSecret { get; init; }
    public string Type { get; init; } = "string";
}

/// <summary>
/// A named environment with parameter value overrides.
/// </summary>
public record PipelineEnvironment
{
    public required string Name { get; init; }
    public Dictionary<string, string> Overrides { get; init; } = new();
}

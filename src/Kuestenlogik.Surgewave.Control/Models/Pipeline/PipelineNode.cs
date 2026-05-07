namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

public record PipelineNode
{
    public required string Id { get; init; }
    public required string ConnectorType { get; init; }
    public required Dictionary<string, string> Config { get; init; }
    public required double X { get; init; }
    public required double Y { get; init; }
    public string? Label { get; init; }
    public NodePlacementConfig? Placement { get; init; }
    public string? SubPipelineId { get; init; }
    public string? PortMappingsJson { get; init; }
    public RetryPolicy? RetryPolicy { get; init; }
    public bool IsComposite => !string.IsNullOrEmpty(SubPipelineId);
}

/// <summary>
/// Placement configuration for a pipeline node (Control UI model).
/// </summary>
public sealed record NodePlacementConfig
{
    /// <summary>Placement strategy: Auto, TagBased, Manual.</summary>
    public string Strategy { get; init; } = "Auto";

    /// <summary>Required worker tags (for TagBased strategy).</summary>
    public string[] RequiredTags { get; init; } = [];

    /// <summary>Explicit WorkerId (for Manual strategy).</summary>
    public string? WorkerId { get; init; }

    /// <summary>Allow auto-install of missing plugins.</summary>
    public bool AllowAutoInstall { get; init; } = true;
}

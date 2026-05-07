namespace Kuestenlogik.Surgewave.Connect.Distributed;

/// <summary>
/// Represents a connector type aggregated from local plugins and remote worker capabilities.
/// Tracks which workers can instantiate this connector type.
/// </summary>
public sealed record AggregatedConnectorType
{
    /// <summary>
    /// Fully qualified class name of the connector.
    /// </summary>
    public required string ClassName { get; init; }

    /// <summary>
    /// Connector type: "source" or "sink".
    /// </summary>
    public required string Type { get; init; }

    /// <summary>
    /// Display name for the pipeline editor UI.
    /// </summary>
    public string? DisplayName { get; init; }

    /// <summary>
    /// MudBlazor icon name for the pipeline editor UI.
    /// </summary>
    public string? Icon { get; init; }

    /// <summary>
    /// Category for grouping in the pipeline editor.
    /// </summary>
    public string? Category { get; init; }

    /// <summary>
    /// Description of what this connector does.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Version of the connector.
    /// </summary>
    public string? Version { get; init; }

    /// <summary>
    /// Worker IDs that can instantiate this connector type.
    /// </summary>
    public IReadOnlyList<string> AvailableOnWorkers { get; init; } = [];

    /// <summary>
    /// Whether this connector type is available locally (in-process).
    /// </summary>
    public bool IsLocal { get; init; }
}

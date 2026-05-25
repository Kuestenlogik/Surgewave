namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Strategy for placing a pipeline node on a worker.
/// </summary>
public enum PlacementStrategy
{
    /// <summary>
    /// Orchestrator chooses automatically based on capabilities and load.
    /// </summary>
    Auto,

    /// <summary>
    /// Automatic, but restricted to workers with specific tags.
    /// </summary>
    TagBased,

    /// <summary>
    /// Explicit worker selection by WorkerId.
    /// </summary>
    Manual
}

/// <summary>
/// Placement configuration for a pipeline node, controlling where it executes.
/// </summary>
public sealed record PlacementConfig
{
    /// <summary>
    /// Placement strategy for worker selection.
    /// </summary>
    public PlacementStrategy Strategy { get; init; } = PlacementStrategy.Auto;

    /// <summary>
    /// Required worker tags when <see cref="Strategy"/> is <see cref="PlacementStrategy.TagBased"/>.
    /// All tags must be present on the worker (AND logic).
    /// </summary>
    public string[] RequiredTags { get; init; } = [];

    /// <summary>
    /// Explicit WorkerId when <see cref="Strategy"/> is <see cref="PlacementStrategy.Manual"/>.
    /// </summary>
    public string? WorkerId { get; init; }

    /// <summary>
    /// Whether to allow auto-installation of missing plugins on the target worker.
    /// Only effective if the target worker also has <c>AllowAutoInstall=true</c>.
    /// </summary>
    public bool AllowAutoInstall { get; init; } = true;
}

/// <summary>
/// Result of a placement decision for a pipeline node.
/// </summary>
public sealed record PlacementResult
{
    /// <summary>Whether a suitable worker was found.</summary>
    public bool Success { get; init; }

    /// <summary>The resolved WorkerId, or null if placement failed.</summary>
    public string? WorkerId { get; init; }

    /// <summary>Error message if placement failed.</summary>
    public string? Error { get; init; }

    /// <summary>Whether a plugin was auto-installed on the target worker.</summary>
    public bool PluginAutoInstalled { get; init; }

    public static PlacementResult Succeeded(string workerId, bool pluginAutoInstalled = false) =>
        new() { Success = true, WorkerId = workerId, PluginAutoInstalled = pluginAutoInstalled };

    public static PlacementResult Failed(string error) =>
        new() { Success = false, Error = error };
}

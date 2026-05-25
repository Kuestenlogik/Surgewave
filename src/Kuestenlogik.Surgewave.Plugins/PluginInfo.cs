namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Discovered plugin information including metadata for the pipeline editor.
/// </summary>
public sealed class PluginInfo
{
    /// <summary>
    /// Fully qualified class name.
    /// </summary>
    public required string Class { get; init; }

    /// <summary>
    /// Plugin type: "source", "sink", "processor", "trigger", "transform", "broker", "protocol", or "unknown".
    /// </summary>
    public required string Type { get; init; }

    /// <summary>
    /// Plugin version.
    /// </summary>
    public required string Version { get; init; }

    /// <summary>
    /// Display name for the pipeline editor UI.
    /// </summary>
    public string? DisplayName { get; init; }

    /// <summary>
    /// MudBlazor icon name for the pipeline editor UI (e.g., "Radar", "Input", "Output").
    /// </summary>
    public string? Icon { get; init; }

    /// <summary>
    /// Category for grouping in the pipeline editor (e.g., "Integration", "Simulation").
    /// </summary>
    public string? Category { get; init; }

    /// <summary>
    /// Description of what this plugin does.
    /// </summary>
    public string? Description { get; init; }
}

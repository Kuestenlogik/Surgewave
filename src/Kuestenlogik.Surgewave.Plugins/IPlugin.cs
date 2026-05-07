namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Base interface for all Surgewave plugins. Every installable component
/// (pipeline nodes, broker plugins, protocol adapters, transforms)
/// implements this interface.
/// </summary>
public interface IPlugin
{
    /// <summary>
    /// Unique feature identifier for this plugin (e.g., fully qualified type name).
    /// </summary>
    string FeatureId { get; }

    /// <summary>
    /// Human-readable display name for the UI.
    /// </summary>
    string DisplayName { get; }
}

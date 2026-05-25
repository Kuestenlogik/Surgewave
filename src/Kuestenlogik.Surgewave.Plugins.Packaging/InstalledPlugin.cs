namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Represents a plugin installed on disk.
/// </summary>
public sealed class InstalledPlugin
{
    public required string Id { get; init; }
    public required string Name { get; init; }
    public required string Version { get; init; }
    public required string InstallPath { get; init; }
    public required PluginManifest Manifest { get; init; }
}

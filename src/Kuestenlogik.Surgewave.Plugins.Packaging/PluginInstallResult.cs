namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Result of a plugin installation operation.
/// </summary>
public sealed class PluginInstallResult
{
    public bool Success { get; private init; }
    public PluginManifest? Manifest { get; private init; }
    public string? InstallPath { get; private init; }
    public string? Error { get; private init; }
    public bool WasUpgrade { get; private init; }
    public string? PreviousVersion { get; private init; }

    public static PluginInstallResult Succeeded(PluginManifest manifest, string installPath,
        bool wasUpgrade = false, string? previousVersion = null) =>
        new()
        {
            Success = true,
            Manifest = manifest,
            InstallPath = installPath,
            WasUpgrade = wasUpgrade,
            PreviousVersion = previousVersion
        };

    public static PluginInstallResult Failed(string error) =>
        new() { Success = false, Error = error };
}

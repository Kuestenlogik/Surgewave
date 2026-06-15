using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Marketplace;

/// <summary>
/// One entry as the wizard sees it — the underlying
/// <see cref="ConnectorPackageInfo"/> from the repository plus a
/// derived <see cref="PluginCategory"/> so the wizard does not
/// re-classify on every render.
/// </summary>
public sealed record PluginMarketplaceEntry
{
    public required ConnectorPackageInfo Package { get; init; }
    public required PluginCategory Category { get; init; }

    public string PackageId => Package.PackageId;
    public string Version => Package.Version;
    public string Name => Package.Name;
    public string? Description => Package.Description;

    /// <summary>
    /// True when the package's manifest declares
    /// <c>surgewaveDependencies</c> on other plugins. Wizard uses this
    /// to decide whether the dependency-resolver fallback needs to run
    /// for this selection.
    /// </summary>
    public bool HasSurgewaveDependencies => Package.HasDependencies;
}

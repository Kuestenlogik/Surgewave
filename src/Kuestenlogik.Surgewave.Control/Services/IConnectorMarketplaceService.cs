using Kuestenlogik.Surgewave.Client.Native.Operations.Plugins;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Service for the connector marketplace UI.
/// Communicates with the broker via Surgewave Native Protocol.
/// </summary>
public interface IConnectorMarketplaceService
{
    /// <summary>
    /// Search for connector packages.
    /// </summary>
    Task<PluginSearchResult> SearchAsync(
        string? query = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get package details.
    /// </summary>
    Task<PluginInfo?> GetPackageAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get dependency tree for a package.
    /// </summary>
    Task<DependencyTreeNode?> GetDependencyTreeAsync(
        string packageId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Install a package with dependencies.
    /// </summary>
    Task<PluginInstallResult> InstallAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Uninstall a package.
    /// </summary>
    Task<PluginUninstallResult> UninstallAsync(
        string packageId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// List installed plugins.
    /// </summary>
    Task<IReadOnlyList<PluginInfo>> ListInstalledAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Upload and install a .swpkg plugin package via the broker's REST API.
    /// </summary>
    Task<PluginUploadResult> UploadPluginAsync(
        Stream packageStream,
        string fileName,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of a plugin upload operation.
/// </summary>
public sealed class PluginUploadResult
{
    public bool IsSuccess { get; init; }
    public string? PluginId { get; init; }
    public string? Version { get; init; }
    public string? Error { get; init; }
    public bool WasUpgrade { get; init; }

    public static PluginUploadResult Success(string pluginId, string version, bool wasUpgrade = false) =>
        new() { IsSuccess = true, PluginId = pluginId, Version = version, WasUpgrade = wasUpgrade };

    public static PluginUploadResult Failed(string error) =>
        new() { IsSuccess = false, Error = error };
}

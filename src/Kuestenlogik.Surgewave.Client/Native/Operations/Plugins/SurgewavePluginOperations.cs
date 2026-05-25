using Kuestenlogik.Surgewave.Client.Native.Commands;
using Kuestenlogik.Surgewave.Client.Native.Commands.Plugins;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Plugins;

/// <summary>
/// Plugin marketplace operations for Surgewave native client.
/// </summary>
public sealed class SurgewavePluginOperations
{
    private readonly CommandExecutor _executor;

    internal SurgewavePluginOperations(SurgewaveNativeClient client)
    {
        _executor = new CommandExecutor(client);
    }

    /// <summary>
    /// Search for plugins in the marketplace.
    /// </summary>
    /// <param name="query">Search query (optional).</param>
    /// <param name="skip">Number of results to skip.</param>
    /// <param name="take">Number of results to take.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Search result with plugins and total count.</returns>
    public Task<PluginSearchResult> SearchAsync(
        string? query = null,
        int skip = 0,
        int take = 20,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new SearchPluginsCommand(query, skip, take), cancellationToken);

    /// <summary>
    /// Get plugin details.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="version">Optional version (latest if not specified).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Plugin info or null if not found.</returns>
    public Task<PluginInfo?> GetPluginAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetPluginCommand(packageId, version), cancellationToken);

    /// <summary>
    /// Install a plugin.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="version">Optional version (latest if not specified).</param>
    /// <param name="includeDependencies">Whether to include dependencies.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Installation result.</returns>
    public Task<PluginInstallResult> InstallAsync(
        string packageId,
        string? version = null,
        bool includeDependencies = true,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new InstallPluginCommand(packageId, version, includeDependencies), cancellationToken);

    /// <summary>
    /// Uninstall a plugin.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="removeDependencies">Whether to remove unused dependencies.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Uninstall result.</returns>
    public Task<PluginUninstallResult> UninstallAsync(
        string packageId,
        bool removeDependencies = false,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new UninstallPluginCommand(packageId, removeDependencies), cancellationToken);

    /// <summary>
    /// List all installed plugins.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of installed plugins.</returns>
    public Task<IReadOnlyList<PluginInfo>> ListInstalledAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new ListInstalledPluginsCommand(), cancellationToken);

    /// <summary>
    /// Get the dependency tree for a plugin.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="version">Optional version.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dependency tree or null if plugin not found.</returns>
    public Task<DependencyTreeNode?> GetDependencyTreeAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetPluginDependenciesCommand(packageId, version), cancellationToken);
}

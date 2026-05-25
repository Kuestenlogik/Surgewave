namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Interface for connector package repositories.
/// </summary>
public interface IConnectorRepository
{
    /// <summary>
    /// Repository name.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Repository source URL.
    /// </summary>
    string Source { get; }

    /// <summary>
    /// Search for connector packages.
    /// </summary>
    /// <param name="query">Search query (name, tags, description).</param>
    /// <param name="skip">Number of results to skip.</param>
    /// <param name="take">Number of results to take.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Search results.</returns>
    Task<IReadOnlyList<ConnectorPackageInfo>> SearchAsync(
        string? query,
        int skip = 0,
        int take = 20,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get details for a specific package.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="version">Optional specific version (latest if not specified).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Package info or null if not found.</returns>
    Task<ConnectorPackageInfo?> GetPackageAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get all versions of a package.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>All available versions.</returns>
    Task<IReadOnlyList<string>> GetVersionsAsync(
        string packageId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Download a package to the specified directory.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="version">Package version.</param>
    /// <param name="targetDirectory">Directory to download to.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Path to the downloaded package.</returns>
    Task<string> DownloadAsync(
        string packageId,
        string version,
        string targetDirectory,
        CancellationToken cancellationToken = default);
}

using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Broker.Plugins;

/// <summary>
/// Manages distribution of installed plugins to connected workers.
/// After a plugin is installed on the broker, this service caches the .swpkg
/// file and can notify workers to pull it via REST.
/// </summary>
public sealed class PluginDistributionService
{
    private readonly string _packageCacheDir;
    private readonly ILogger<PluginDistributionService> _logger;

    public PluginDistributionService(string packageCacheDir, ILogger<PluginDistributionService> logger)
    {
        _packageCacheDir = packageCacheDir;
        _logger = logger;
        Directory.CreateDirectory(_packageCacheDir);
    }

    /// <summary>
    /// Called after a plugin is successfully installed. Caches the package
    /// for worker download and returns the notification payload.
    /// </summary>
    public async Task<PluginDistributionInfo?> OnPluginInstalledAsync(
        string packagePath,
        PluginInstallResult result,
        CancellationToken cancellationToken = default)
    {
        if (!result.Success || result.Manifest == null)
            return null;

        var manifest = result.Manifest;
        var cachedFileName = $"{manifest.Id}-{manifest.Version}.swpkg";
        var cachedPath = Path.Combine(_packageCacheDir, cachedFileName);

        // Cache the package for worker download
        if (!File.Exists(cachedPath) && File.Exists(packagePath))
        {
            File.Copy(packagePath, cachedPath, overwrite: true);
            _logger.LogInformation("Cached plugin package for distribution: {FileName}", cachedFileName);
        }

        // Compute SHA256
        var sha256 = await PackageChecksumCalculator.ComputeAsync(cachedPath, cancellationToken);

        return new PluginDistributionInfo
        {
            PluginId = manifest.Id,
            Version = manifest.Version,
            Sha256 = sha256
        };
    }

    /// <summary>
    /// Gets the list of all cached plugin packages available for download.
    /// </summary>
    public IReadOnlyList<PluginDistributionInfo> GetAvailablePackages()
    {
        var packages = new List<PluginDistributionInfo>();

        if (!Directory.Exists(_packageCacheDir))
            return packages;

        foreach (var file in Directory.GetFiles(_packageCacheDir, "*.swpkg"))
        {
            var fileName = Path.GetFileNameWithoutExtension(file);
            var lastDash = fileName.LastIndexOf('-');
            if (lastDash > 0)
            {
                packages.Add(new PluginDistributionInfo
                {
                    PluginId = fileName[..lastDash],
                    Version = fileName[(lastDash + 1)..],
                    Sha256 = ""
                });
            }
        }

        return packages;
    }
}

/// <summary>
/// Information about a plugin available for distribution to workers.
/// </summary>
public sealed class PluginDistributionInfo
{
    public required string PluginId { get; init; }
    public required string Version { get; init; }
    public required string Sha256 { get; init; }
}

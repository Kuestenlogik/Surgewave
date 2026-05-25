using NuGet.Common;
using NuGet.Configuration;
using NuGet.Packaging.Core;
using NuGet.Protocol;
using NuGet.Protocol.Core.Types;
using NuGet.Versioning;

namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Connector repository backed by NuGet package source.
/// </summary>
public sealed class NuGetConnectorRepository : IConnectorRepository, IDisposable
{
    private readonly SourceRepository _repository;
    private readonly SourceCacheContext _cacheContext;
    private readonly ILogger _logger;
    private readonly string _packagePrefix;

    /// <summary>
    /// Creates a new NuGet connector repository.
    /// </summary>
    /// <param name="name">Repository name.</param>
    /// <param name="source">NuGet source URL.</param>
    /// <param name="packagePrefix">Package ID prefix for Surgewave connectors.</param>
    public NuGetConnectorRepository(string name, string source, string packagePrefix = "Kuestenlogik.Surgewave.Connector.")
    {
        Name = name;
        Source = source;
        _packagePrefix = packagePrefix;
        _cacheContext = new SourceCacheContext();
        _logger = NullLogger.Instance;

        var packageSource = new PackageSource(source);
        _repository = NuGet.Protocol.Core.Types.Repository.Factory.GetCoreV3(packageSource);
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public string Source { get; }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ConnectorPackageInfo>> SearchAsync(
        string? query,
        int skip = 0,
        int take = 20,
        CancellationToken cancellationToken = default)
    {
        var searchResource = await _repository.GetResourceAsync<PackageSearchResource>(cancellationToken);
        var filter = new SearchFilter(includePrerelease: false);

        // Prepend prefix to query to filter for Surgewave connectors
        var searchQuery = string.IsNullOrWhiteSpace(query)
            ? _packagePrefix
            : $"{_packagePrefix} {query}";

        var results = await searchResource.SearchAsync(
            searchQuery,
            filter,
            skip,
            take,
            _logger,
            cancellationToken);

        var packages = new List<ConnectorPackageInfo>();

        foreach (var result in results)
        {
            if (!result.Identity.Id.StartsWith(_packagePrefix, StringComparison.OrdinalIgnoreCase))
                continue;

            var versions = await result.GetVersionsAsync();
            var versionList = versions
                .Select(v => v.Version.ToNormalizedString())
                .OrderByDescending(v => NuGetVersion.Parse(v))
                .ToList();

            packages.Add(new ConnectorPackageInfo
            {
                PackageId = result.Identity.Id,
                Version = result.Identity.Version.ToNormalizedString(),
                Name = GetConnectorName(result.Identity.Id),
                Description = result.Description,
                Author = result.Authors,
                IconUrl = result.IconUrl?.ToString(),
                ProjectUrl = result.ProjectUrl?.ToString(),
                License = result.LicenseUrl?.ToString(),
                Tags = ParseTags(result.Tags),
                AvailableVersions = versionList,
                DownloadCount = result.DownloadCount ?? 0,
                Published = result.Published,
                ConnectorTypes = InferConnectorTypes(result.Tags)
            });
        }

        return packages;
    }

    /// <inheritdoc />
    public async Task<ConnectorPackageInfo?> GetPackageAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        var metadataResource = await _repository.GetResourceAsync<PackageMetadataResource>(cancellationToken);

        var packages = await metadataResource.GetMetadataAsync(
            packageId,
            includePrerelease: false,
            includeUnlisted: false,
            _cacheContext,
            _logger,
            cancellationToken);

        var packageList = packages.ToList();
        if (packageList.Count == 0) return null;

        IPackageSearchMetadata package;
        if (string.IsNullOrWhiteSpace(version))
        {
            package = packageList.OrderByDescending(p => p.Identity.Version).First();
        }
        else
        {
            var targetVersion = NuGetVersion.Parse(version);
            package = packageList.FirstOrDefault(p => p.Identity.Version == targetVersion)
                      ?? packageList.OrderByDescending(p => p.Identity.Version).First();
        }

        var versions = packageList
            .Select(p => p.Identity.Version.ToNormalizedString())
            .OrderByDescending(v => NuGetVersion.Parse(v))
            .ToList();

        return new ConnectorPackageInfo
        {
            PackageId = package.Identity.Id,
            Version = package.Identity.Version.ToNormalizedString(),
            Name = GetConnectorName(package.Identity.Id),
            Description = package.Description,
            Author = package.Authors,
            IconUrl = package.IconUrl?.ToString(),
            ProjectUrl = package.ProjectUrl?.ToString(),
            License = package.LicenseUrl?.ToString(),
            Tags = ParseTags(package.Tags),
            AvailableVersions = versions,
            DownloadCount = package.DownloadCount ?? 0,
            Published = package.Published,
            ConnectorTypes = InferConnectorTypes(package.Tags)
        };
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> GetVersionsAsync(
        string packageId,
        CancellationToken cancellationToken = default)
    {
        var findResource = await _repository.GetResourceAsync<FindPackageByIdResource>(cancellationToken);
        var versions = await findResource.GetAllVersionsAsync(
            packageId,
            _cacheContext,
            _logger,
            cancellationToken);

        return versions
            .OrderByDescending(v => v)
            .Select(v => v.ToNormalizedString())
            .ToList();
    }

    /// <inheritdoc />
    public async Task<string> DownloadAsync(
        string packageId,
        string version,
        string targetDirectory,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(targetDirectory);

        var downloadResource = await _repository.GetResourceAsync<DownloadResource>(cancellationToken);
        var packageVersion = NuGetVersion.Parse(version);
        var identity = new PackageIdentity(packageId, packageVersion);

        var downloadResult = await downloadResource.GetDownloadResourceResultAsync(
            identity,
            new PackageDownloadContext(_cacheContext),
            globalPackagesFolder: null!,
            _logger,
            cancellationToken);

        if (downloadResult.Status != DownloadResourceResultStatus.Available)
        {
            throw new InvalidOperationException($"Failed to download package {packageId} {version}");
        }

        var targetPath = Path.Combine(targetDirectory, $"{packageId}.{version}.nupkg");
        using (var fileStream = File.Create(targetPath))
        {
            await downloadResult.PackageStream.CopyToAsync(fileStream, cancellationToken);
        }

        return targetPath;
    }

    private static string GetConnectorName(string packageId)
    {
        // Extract name from package ID like "Kuestenlogik.Surgewave.Connector.Hue" -> "Hue"
        var parts = packageId.Split('.');
        if (parts.Length >= 4)
        {
            return string.Join(" ", parts.Skip(3));
        }
        return packageId;
    }

    private static string[] ParseTags(string? tags)
    {
        if (string.IsNullOrWhiteSpace(tags)) return [];
        return tags.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    private static List<string> InferConnectorTypes(string? tags)
    {
        if (string.IsNullOrWhiteSpace(tags)) return ["source", "sink"];

        var types = new List<string>();
        var lowerTags = tags.ToLowerInvariant();

        if (lowerTags.Contains("source"))
            types.Add("source");
        if (lowerTags.Contains("sink"))
            types.Add("sink");

        return types.Count > 0 ? types : ["source", "sink"];
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _cacheContext.Dispose();
    }
}

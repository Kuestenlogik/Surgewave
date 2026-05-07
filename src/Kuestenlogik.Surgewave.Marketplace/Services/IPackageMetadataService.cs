using Kuestenlogik.Surgewave.Marketplace.Models;

namespace Kuestenlogik.Surgewave.Marketplace.Services;

/// <summary>
/// Abstraction for package metadata storage and retrieval.
/// </summary>
public interface IPackageMetadataService
{
    Task<PackageMetadata?> GetAsync(string id, string? version = null, CancellationToken ct = default);
    Task<IReadOnlyList<PackageMetadata>> SearchAsync(string? query, int skip, int take, CancellationToken ct = default);
    Task SaveAsync(PackageMetadata metadata, CancellationToken ct = default);
    Task DeleteAsync(string id, string version, CancellationToken ct = default);
    Task<IReadOnlyList<string>> GetVersionsAsync(string id, CancellationToken ct = default);
    Task<PackageStatistics> GetStatisticsAsync(CancellationToken ct = default);
    Task IncrementDownloadCountAsync(string id, string version, CancellationToken ct = default);
    Task InitializeAsync(CancellationToken ct = default);
}

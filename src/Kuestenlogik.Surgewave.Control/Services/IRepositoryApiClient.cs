using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// HTTP client over the broker's <c>/api/plugins/repositories</c> surface.
/// Uses the canonical <see cref="RepositoryEntry"/> shape from the
/// <c>Kuestenlogik.Surgewave.Plugins.Repository</c> package so a single
/// schema travels from Control's MudTable into the broker's JSON store.
/// </summary>
public interface IRepositoryApiClient
{
    Task<IReadOnlyList<RepositoryEntry>> ListAsync(CancellationToken ct = default);

    Task<RepositoryEntry> AddAsync(RepositoryEntry entry, CancellationToken ct = default);

    Task<RepositoryEntry> UpdateAsync(string name, RepositoryEntry entry, CancellationToken ct = default);

    Task DeleteAsync(string name, CancellationToken ct = default);
}

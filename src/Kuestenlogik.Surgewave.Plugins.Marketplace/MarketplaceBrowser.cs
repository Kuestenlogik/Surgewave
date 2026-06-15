using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Marketplace;

/// <summary>
/// Wizard-facing browse API over one or more <see cref="IConnectorRepository"/>
/// sources (typically the configured NuGet feed). Adds three things on
/// top of the raw repository search:
///
/// <list type="bullet">
///   <item>
///     <description>
///       Aggregates across multiple repos so the wizard does not have
///       to iterate them itself.
///     </description>
///   </item>
///   <item>
///     <description>
///       Classifies each result into a <see cref="PluginCategory"/>
///       bucket (Storage / Connector / Protocol / …).
///     </description>
///   </item>
///   <item>
///     <description>
///       Optional in-memory cache per browser instance so the wizard's
///       "Next / Back" navigation does not re-issue the search on every
///       step.
///     </description>
///   </item>
/// </list>
///
/// The lean variant intentionally does NOT maintain a separate
/// marketplace index file or hosted service — it leans on NuGet's
/// existing search API for everything. Issues #21 and #22 (the two
/// setup-wizard variants) consume this directly.
/// </summary>
public sealed class MarketplaceBrowser
{
    private readonly IReadOnlyList<IConnectorRepository> _repositories;
    private IReadOnlyList<PluginMarketplaceEntry>? _cache;

    public MarketplaceBrowser(params IConnectorRepository[] repositories)
        : this((IReadOnlyList<IConnectorRepository>)repositories) { }

    public MarketplaceBrowser(IReadOnlyList<IConnectorRepository> repositories)
    {
        _repositories = repositories;
    }

    /// <summary>
    /// All Surgewave-tagged plugins across the configured repositories,
    /// classified into wizard buckets. Cached after the first call;
    /// invalidate via <see cref="RefreshAsync"/> when the user clicks
    /// "Reload".
    /// </summary>
    public async Task<IReadOnlyList<PluginMarketplaceEntry>> BrowseAsync(
        string? query = null,
        PluginCategory? categoryFilter = null,
        int take = 100,
        CancellationToken cancellationToken = default)
    {
        var entries = _cache ?? await LoadAllAsync(take, cancellationToken).ConfigureAwait(false);
        _cache ??= entries;

        IEnumerable<PluginMarketplaceEntry> filtered = entries;
        if (categoryFilter is not null)
        {
            filtered = filtered.Where(e => e.Category == categoryFilter.Value);
        }
        if (!string.IsNullOrWhiteSpace(query))
        {
            filtered = filtered.Where(e =>
                e.PackageId.Contains(query, StringComparison.OrdinalIgnoreCase)
                || (e.Description ?? "").Contains(query, StringComparison.OrdinalIgnoreCase)
                || e.Name.Contains(query, StringComparison.OrdinalIgnoreCase));
        }
        return filtered.ToList();
    }

    /// <summary>Drop the cache so the next <see cref="BrowseAsync"/> hits the repositories again.</summary>
    public Task RefreshAsync()
    {
        _cache = null;
        return Task.CompletedTask;
    }

    /// <summary>
    /// Group the wizard-selected entries by category — the standard
    /// shape the install-script generator (#21) consumes.
    /// </summary>
    public static IReadOnlyDictionary<PluginCategory, IReadOnlyList<PluginMarketplaceEntry>> GroupByCategory(
        IEnumerable<PluginMarketplaceEntry> entries)
    {
        return entries
            .GroupBy(e => e.Category)
            .ToDictionary(
                g => g.Key,
                g => (IReadOnlyList<PluginMarketplaceEntry>)g.ToList());
    }

    private async Task<IReadOnlyList<PluginMarketplaceEntry>> LoadAllAsync(int take, CancellationToken ct)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var entries = new List<PluginMarketplaceEntry>();
        foreach (var repo in _repositories)
        {
            // The "surgewave" string is the canonical tag for marketplace
            // discovery — both the v1 manifest schema convention and the
            // existing CLI's `plugins search` rely on it.
            var hits = await repo.SearchAsync(query: "surgewave", skip: 0, take: take, ct).ConfigureAwait(false);
            foreach (var hit in hits)
            {
                if (!seen.Add(hit.PackageId)) continue; // dedup across repos
                entries.Add(new PluginMarketplaceEntry
                {
                    Package = hit,
                    Category = PluginCategoryClassifier.Classify(hit.Tags),
                });
            }
        }
        return entries;
    }
}

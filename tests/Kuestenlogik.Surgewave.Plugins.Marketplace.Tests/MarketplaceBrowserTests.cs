using Kuestenlogik.Surgewave.Plugins.Marketplace;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Xunit;

namespace Kuestenlogik.Surgewave.Plugins.Marketplace.Tests;

public sealed class MarketplaceBrowserTests
{
    [Fact]
    public async Task BrowseAsync_returns_all_entries_classified_by_tag()
    {
        var repo = new FakeRepository("nuget",
            Pkg("Surgewave.Storage.NvmeDirect", "storage-engine"),
            Pkg("Surgewave.Connector.Postgres", "connector", "source"),
            Pkg("Surgewave.Protocol.Mqtt", "protocol"));
        var sut = new MarketplaceBrowser(repo);

        var entries = await sut.BrowseAsync();

        Assert.Equal(3, entries.Count);
        Assert.Equal(PluginCategory.StorageEngine, entries.Single(e => e.PackageId == "Surgewave.Storage.NvmeDirect").Category);
        Assert.Equal(PluginCategory.Connector,    entries.Single(e => e.PackageId == "Surgewave.Connector.Postgres").Category);
        Assert.Equal(PluginCategory.Protocol,     entries.Single(e => e.PackageId == "Surgewave.Protocol.Mqtt").Category);
    }

    [Fact]
    public async Task BrowseAsync_filters_by_category()
    {
        var repo = new FakeRepository("nuget",
            Pkg("Surgewave.Storage.A", "storage-engine"),
            Pkg("Surgewave.Connector.B", "connector"));
        var sut = new MarketplaceBrowser(repo);

        var storage = await sut.BrowseAsync(categoryFilter: PluginCategory.StorageEngine);

        Assert.Single(storage);
        Assert.Equal("Surgewave.Storage.A", storage[0].PackageId);
    }

    [Fact]
    public async Task BrowseAsync_filters_by_query_across_name_id_description()
    {
        var repo = new FakeRepository("nuget",
            Pkg("Surgewave.Storage.NvmeDirect", [ "storage-engine" ], description: "NVMe direct I/O"),
            Pkg("Surgewave.Connector.Postgres", [ "connector" ], description: "Postgres CDC source"));
        var sut = new MarketplaceBrowser(repo);

        Assert.Single(await sut.BrowseAsync(query: "Postgres"));
        Assert.Single(await sut.BrowseAsync(query: "NVMe"));   // description hit
        Assert.Empty(await sut.BrowseAsync(query: "Cassandra"));
    }

    [Fact]
    public async Task BrowseAsync_dedups_packages_across_repositories()
    {
        var primary = new FakeRepository("primary",   Pkg("Surgewave.A", "connector"));
        var mirror  = new FakeRepository("mirror",    Pkg("Surgewave.A", "connector"), Pkg("Surgewave.B", "protocol"));
        var sut = new MarketplaceBrowser(primary, mirror);

        var entries = await sut.BrowseAsync();

        Assert.Equal(2, entries.Count);
        Assert.Contains(entries, e => e.PackageId == "Surgewave.A");
        Assert.Contains(entries, e => e.PackageId == "Surgewave.B");
    }

    [Fact]
    public async Task BrowseAsync_caches_results_until_RefreshAsync()
    {
        var repo = new FakeRepository("nuget", Pkg("Surgewave.A", "connector"));
        var sut = new MarketplaceBrowser(repo);

        await sut.BrowseAsync();
        await sut.BrowseAsync();
        Assert.Equal(1, repo.SearchCallCount);

        await sut.RefreshAsync();
        await sut.BrowseAsync();
        Assert.Equal(2, repo.SearchCallCount);
    }

    [Fact]
    public void GroupByCategory_buckets_entries_for_the_install_script_generator()
    {
        var entries = new[]
        {
            Entry("Surgewave.S1", PluginCategory.StorageEngine),
            Entry("Surgewave.C1", PluginCategory.Connector),
            Entry("Surgewave.C2", PluginCategory.Connector),
        };

        var grouped = MarketplaceBrowser.GroupByCategory(entries);

        Assert.Single(grouped[PluginCategory.StorageEngine]);
        Assert.Equal(2, grouped[PluginCategory.Connector].Count);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static ConnectorPackageInfo Pkg(string id, params string[] tags) => Pkg(id, tags, description: null);

    private static ConnectorPackageInfo Pkg(string id, string[] tags, string? description) =>
        new()
        {
            PackageId = id,
            Version = "1.0.0",
            Name = id,
            Description = description,
            Tags = tags,
        };

    private static PluginMarketplaceEntry Entry(string id, PluginCategory category) =>
        new()
        {
            Package = new ConnectorPackageInfo { PackageId = id, Version = "1.0.0", Name = id },
            Category = category,
        };

    private sealed class FakeRepository : IConnectorRepository
    {
        private readonly ConnectorPackageInfo[] _packages;
        public int SearchCallCount { get; private set; }

        public FakeRepository(string name, params ConnectorPackageInfo[] packages)
        {
            Name = name;
            _packages = packages;
        }

        public string Name { get; }
        public string Source => $"fake://{Name}";

        public Task<IReadOnlyList<ConnectorPackageInfo>> SearchAsync(string? query, int skip = 0, int take = 20, CancellationToken cancellationToken = default)
        {
            SearchCallCount++;
            return Task.FromResult<IReadOnlyList<ConnectorPackageInfo>>(_packages);
        }

        public Task<ConnectorPackageInfo?> GetPackageAsync(string packageId, string? version = null, CancellationToken cancellationToken = default) =>
            Task.FromResult<ConnectorPackageInfo?>(_packages.FirstOrDefault(p => p.PackageId == packageId));

        public Task<IReadOnlyList<string>> GetVersionsAsync(string packageId, CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<string>>([ "1.0.0" ]);

        public Task<string> DownloadAsync(string packageId, string version, string targetDirectory, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}

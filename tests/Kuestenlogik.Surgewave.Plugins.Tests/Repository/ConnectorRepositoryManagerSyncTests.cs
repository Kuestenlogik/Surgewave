using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Repository;

/// <summary>
/// Covers <see cref="ConnectorRepositoryManager.SyncFromStore"/> — the bridge
/// that makes operator-edited <c>RepositoryStore</c> entries actually drive
/// the broker's marketplace-search repositories. Without this glue, edits in
/// <c>/plugins/sources</c> would silently never affect SearchPlugins results.
/// </summary>
public sealed class ConnectorRepositoryManagerSyncTests : IDisposable
{
    private readonly string _root;
    private readonly string _installDir;
    private readonly string _configPath;

    public ConnectorRepositoryManagerSyncTests()
    {
        _root = Directory.CreateTempSubdirectory("surgewave-repomgr-").FullName;
        _installDir = Path.Combine(_root, "install");
        _configPath = Path.Combine(_root, "repos.json");
        Directory.CreateDirectory(_installDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    private static RepositoryEntry Entry(string name, RepositoryType type = RepositoryType.NuGet, bool enabled = true) =>
        new() { Name = name, Type = type, Source = "https://example.com/feed", Enabled = enabled };

    [Fact]
    public void SyncFromStore_ReplacesBuiltinDefault_WithStoreEntries()
    {
        using var mgr = new ConnectorRepositoryManager(_installDir);
        var before = mgr.Repositories.Select(r => r.Name).ToList();
        // Constructor seeds nuget.org as the hardcoded default — sanity check
        // so the test actually exercises a replacement.
        Assert.Contains("nuget.org", before);

        var store = new RepositoryStore(_configPath);
        // Store seeds RepositoryConfiguration.CreateDefault() on first read
        // (nuget.org + surgewave-connectors), so simply call List() then add
        // our own entry on top.
        store.Add(Entry("acme.private", RepositoryType.Http));

        mgr.SyncFromStore(store);

        var after = mgr.Repositories.Select(r => r.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        Assert.Contains("nuget.org", after);
        Assert.Contains("surgewave-connectors", after);
        Assert.Contains("acme.private", after);
        // The hardcoded default that the constructor added must be gone (it
        // gets re-seeded by the store, so the count stays sane).
        Assert.Equal(3, mgr.Repositories.Count);
    }

    [Fact]
    public void SyncFromStore_SkipsDisabledEntries()
    {
        using var mgr = new ConnectorRepositoryManager(_installDir);
        var store = new RepositoryStore(_configPath);
        store.Add(Entry("muted", enabled: false));

        mgr.SyncFromStore(store);

        Assert.DoesNotContain(mgr.Repositories, r => r.Name == "muted");
    }

    [Fact]
    public void SyncFromStore_TwiceIsIdempotent()
    {
        using var mgr = new ConnectorRepositoryManager(_installDir);
        var store = new RepositoryStore(_configPath);
        store.Add(Entry("acme"));

        mgr.SyncFromStore(store);
        var firstCount = mgr.Repositories.Count;
        mgr.SyncFromStore(store);

        Assert.Equal(firstCount, mgr.Repositories.Count);
    }

    [Fact]
    public void SyncFromStore_AfterStoreMutates_ReflectsChange()
    {
        using var mgr = new ConnectorRepositoryManager(_installDir);
        var store = new RepositoryStore(_configPath);
        store.Add(Entry("ephemeral"));
        mgr.SyncFromStore(store);
        Assert.Contains(mgr.Repositories, r => r.Name == "ephemeral");

        store.Remove("ephemeral");
        mgr.SyncFromStore(store);
        Assert.DoesNotContain(mgr.Repositories, r => r.Name == "ephemeral");
    }

    [Fact]
    public void SyncFromStore_NullStore_Throws()
    {
        using var mgr = new ConnectorRepositoryManager(_installDir);
        Assert.Throws<ArgumentNullException>(() => mgr.SyncFromStore(null!));
    }
}

using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Repository;

/// <summary>
/// Mtime-based live resync: <see cref="ConnectorRepositoryManager.EnsureSynced"/>
/// must pick up REST-driven store mutations on the next search without a
/// broker restart, but stay a no-op when nothing changed so we don't pay the
/// disposal/re-add cost on every query.
/// </summary>
public sealed class ConnectorRepositoryManagerLiveResyncTests : IDisposable
{
    private readonly string _root;
    private readonly string _installDir;
    private readonly string _configPath;

    public ConnectorRepositoryManagerLiveResyncTests()
    {
        _root = Directory.CreateTempSubdirectory("surgewave-resync-").FullName;
        _installDir = Path.Combine(_root, "install");
        _configPath = Path.Combine(_root, "repos.json");
        Directory.CreateDirectory(_installDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    private static RepositoryEntry Entry(string name) =>
        new() { Name = name, Type = RepositoryType.Http, Source = $"https://example.com/{name}", Enabled = true };

    [Fact]
    public void EnsureSynced_AfterStoreMutate_PicksUpNewEntry()
    {
        using var mgr = new ConnectorRepositoryManager(_installDir);
        var store = new RepositoryStore(_configPath);
        mgr.SyncFromStore(store);
        var initial = mgr.Repositories.Count;

        // Sleep briefly to guarantee a different file mtime — some filesystems
        // truncate to 10ms / 1s granularity; 20ms is enough on Windows/Linux.
        Thread.Sleep(20);
        store.Add(Entry("late-arrival"));

        mgr.EnsureSynced();

        Assert.Equal(initial + 1, mgr.Repositories.Count);
        Assert.Contains(mgr.Repositories, r => r.Name == "late-arrival");
    }

    [Fact]
    public void EnsureSynced_AfterStoreRemove_PicksUpDeletion()
    {
        using var mgr = new ConnectorRepositoryManager(_installDir);
        var store = new RepositoryStore(_configPath);
        store.Add(Entry("ephemeral"));
        mgr.SyncFromStore(store);
        Assert.Contains(mgr.Repositories, r => r.Name == "ephemeral");

        Thread.Sleep(20);
        store.Remove("ephemeral");
        mgr.EnsureSynced();

        Assert.DoesNotContain(mgr.Repositories, r => r.Name == "ephemeral");
    }

    [Fact]
    public void EnsureSynced_WithoutAssociatedStore_IsNoop()
    {
        using var mgr = new ConnectorRepositoryManager(_installDir);
        var before = mgr.Repositories.Count;
        mgr.EnsureSynced();
        Assert.Equal(before, mgr.Repositories.Count);
    }

    [Fact]
    public async Task SearchAsync_AfterStoreMutate_ReflectsNewEntries()
    {
        using var mgr = new ConnectorRepositoryManager(_installDir);
        var store = new RepositoryStore(_configPath);
        mgr.SyncFromStore(store);
        var initial = mgr.Repositories.Count;

        Thread.Sleep(20);
        store.Add(Entry("late-arrival"));

        // SearchAsync calls EnsureSynced internally — no manual hook needed.
        // The repos themselves may fail to actually search (fake URL), but
        // that's caught and ignored inside the manager; the assertion is
        // about the repo list being refreshed.
        await mgr.SearchAsync("anything");
        Assert.Equal(initial + 1, mgr.Repositories.Count);
    }
}

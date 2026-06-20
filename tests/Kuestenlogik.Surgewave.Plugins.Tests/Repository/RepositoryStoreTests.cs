using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Repository;

/// <summary>
/// Covers <see cref="RepositoryStore"/>: CRUD over the on-disk
/// surgewave-repositories.json plus the entry-name + URL validation that
/// protects the broker REST surface from malformed input.
/// </summary>
public sealed class RepositoryStoreTests : IDisposable
{
    private readonly string _root;
    private readonly string _configPath;
    private readonly RepositoryStore _store;

    public RepositoryStoreTests()
    {
        _root = Directory.CreateTempSubdirectory("surgewave-repostore-").FullName;
        _configPath = Path.Combine(_root, "surgewave-repositories.json");
        _store = new RepositoryStore(_configPath);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    private static RepositoryEntry Entry(string name, string source = "https://example.com/feed", bool enabled = true) =>
        new() { Name = name, Type = RepositoryType.NuGet, Source = source, Enabled = enabled };

    [Fact]
    public void List_OnFirstRun_SeedsDefaultsAndPersists()
    {
        var entries = _store.List();
        Assert.NotEmpty(entries);
        Assert.True(File.Exists(_configPath), "Store should seed the file on first read.");
    }

    [Fact]
    public void Add_NewEntry_PersistsAndShowsInList()
    {
        var saved = _store.Add(Entry("mycompany"));
        Assert.Equal("mycompany", saved.Name);

        // Re-instantiate to prove it survived the round-trip.
        var fresh = new RepositoryStore(_configPath);
        Assert.Contains(fresh.List(), r => r.Name == "mycompany");
    }

    [Fact]
    public void Add_DuplicateName_Throws()
    {
        _store.Add(Entry("dupe"));
        Assert.Throws<InvalidOperationException>(() => _store.Add(Entry("dupe", source: "https://other.example/feed")));
    }

    [Fact]
    public void Update_ChangesPersistedFields()
    {
        _store.Add(Entry("toggleable"));
        var updated = _store.Update("toggleable", Entry("toggleable", enabled: false));
        Assert.False(updated.Enabled);
        var loaded = _store.Get("toggleable")!;
        Assert.False(loaded.Enabled);
    }

    [Fact]
    public void Update_RenameAttempt_Throws()
    {
        _store.Add(Entry("alpha"));
        Assert.Throws<ArgumentException>(() => _store.Update("alpha", Entry("beta")));
    }

    [Fact]
    public void Update_UnknownName_Throws()
    {
        Assert.Throws<KeyNotFoundException>(() => _store.Update("ghost", Entry("ghost")));
    }

    [Fact]
    public void Remove_KnownEntry_ReturnsTrueAndPersists()
    {
        _store.Add(Entry("trash"));
        Assert.True(_store.Remove("trash"));
        Assert.Null(_store.Get("trash"));
    }

    [Fact]
    public void Remove_UnknownEntry_ReturnsFalse()
    {
        Assert.False(_store.Remove("never-existed"));
    }

    [Theory]
    [InlineData("with/slash")]
    [InlineData("with\\backslash")]
    [InlineData("")]
    [InlineData("   ")]
    public void Add_InvalidName_Throws(string name)
    {
        Assert.ThrowsAny<ArgumentException>(() => _store.Add(Entry(name)));
    }

    [Fact]
    public void Add_NonAbsoluteSource_Throws()
    {
        Assert.Throws<ArgumentException>(() => _store.Add(Entry("relative", source: "not-a-url")));
    }

    [Fact]
    public void List_ReturnsClones_NotInternalReferences()
    {
        _store.Add(Entry("clone-check"));
        var first = _store.List();
        first[0].Enabled = !first[0].Enabled;   // mutate the snapshot
        var second = _store.List();
        Assert.NotEqual(first[0].Enabled, second.Single(r => r.Name == "clone-check").Enabled);
    }
}

using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

/// <summary>
/// Coverage for <see cref="RepositoryConfiguration"/> — die Persistenz-Schicht der
/// `surgewave plugins source`-Subcommands. Roundtrip, Add/Remove, CreateRepositories
/// factory, plus die Defaults die ueberall greifen wo kein User-Override existiert.
/// </summary>
public sealed class RepositoryConfigurationTests : IDisposable
{
    private readonly string _tempDir;

    public RepositoryConfigurationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sw-repocfg-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    [Fact]
    public void CreateDefault_HasNuGetAndSurgewaveConnectorsAsDefaults()
    {
        var cfg = RepositoryConfiguration.CreateDefault();

        Assert.Equal("nuget.org", cfg.DefaultRepository);
        Assert.Equal(2, cfg.Repositories.Count);
        Assert.Contains(cfg.Repositories, r => r.Name == "nuget.org" && r.Type == RepositoryType.NuGet);
        Assert.Contains(cfg.Repositories, r => r.Name == "surgewave-connectors" && r.Type == RepositoryType.Http);
        Assert.All(cfg.Repositories, r => Assert.True(r.Enabled));
    }

    [Fact]
    public void AddRepository_AppendsNewEntry()
    {
        var cfg = new RepositoryConfiguration();

        cfg.AddRepository(new RepositoryEntry { Name = "test", Source = "https://example.com" });

        Assert.Single(cfg.Repositories);
        Assert.Equal("test", cfg.Repositories[0].Name);
    }

    [Fact]
    public void AddRepository_DuplicateName_ReplacesExisting()
    {
        var cfg = new RepositoryConfiguration();
        cfg.AddRepository(new RepositoryEntry { Name = "test", Source = "https://old.com" });

        cfg.AddRepository(new RepositoryEntry { Name = "test", Source = "https://new.com" });

        Assert.Single(cfg.Repositories);
        Assert.Equal("https://new.com", cfg.Repositories[0].Source);
    }

    [Fact]
    public void AddRepository_DuplicateName_CaseInsensitive()
    {
        var cfg = new RepositoryConfiguration();
        cfg.AddRepository(new RepositoryEntry { Name = "TEST", Source = "https://old.com" });

        cfg.AddRepository(new RepositoryEntry { Name = "test", Source = "https://new.com" });

        Assert.Single(cfg.Repositories);
    }

    [Fact]
    public void RemoveRepository_RemovesAndReturnsTrue()
    {
        var cfg = new RepositoryConfiguration();
        cfg.AddRepository(new RepositoryEntry { Name = "a", Source = "x" });
        cfg.AddRepository(new RepositoryEntry { Name = "b", Source = "y" });

        var removed = cfg.RemoveRepository("a");

        Assert.True(removed);
        Assert.Single(cfg.Repositories);
        Assert.Equal("b", cfg.Repositories[0].Name);
    }

    [Fact]
    public void RemoveRepository_NotExisting_ReturnsFalse()
    {
        var cfg = new RepositoryConfiguration();
        cfg.AddRepository(new RepositoryEntry { Name = "a", Source = "x" });

        var removed = cfg.RemoveRepository("does-not-exist");

        Assert.False(removed);
        Assert.Single(cfg.Repositories);
    }

    [Fact]
    public void RemoveRepository_CaseInsensitive()
    {
        var cfg = new RepositoryConfiguration();
        cfg.AddRepository(new RepositoryEntry { Name = "TestRepo", Source = "x" });

        var removed = cfg.RemoveRepository("testrepo");

        Assert.True(removed);
        Assert.Empty(cfg.Repositories);
    }

    [Fact]
    public void SaveTo_LoadFrom_Roundtrip_PreservesAllEntries()
    {
        var original = RepositoryConfiguration.CreateDefault();
        original.AddRepository(new RepositoryEntry
        {
            Name = "private",
            Source = "https://nuget.pkg.github.com/Kuestenlogik/index.json",
            Type = RepositoryType.NuGet,
            PackagePrefix = "Kuestenlogik.Surgewave.",
            Credentials = new RepositoryCredentials { Token = "ghp_secret" },
        });
        var path = Path.Combine(_tempDir, "repo.json");

        original.SaveTo(path);
        var loaded = RepositoryConfiguration.LoadFrom(path);

        Assert.Equal(original.DefaultRepository, loaded.DefaultRepository);
        Assert.Equal(original.Repositories.Count, loaded.Repositories.Count);

        var privateRepo = loaded.Repositories.Single(r => r.Name == "private");
        Assert.Equal(RepositoryType.NuGet, privateRepo.Type);
        Assert.Equal("Kuestenlogik.Surgewave.", privateRepo.PackagePrefix);
        Assert.NotNull(privateRepo.Credentials);
        Assert.Equal("ghp_secret", privateRepo.Credentials!.Token);
    }

    [Fact]
    public void SaveTo_CreatesParentDirectory()
    {
        var nestedPath = Path.Combine(_tempDir, "deep", "nested", "repo.json");
        var cfg = RepositoryConfiguration.CreateDefault();

        cfg.SaveTo(nestedPath);

        Assert.True(File.Exists(nestedPath));
    }

    [Fact]
    public void LoadFrom_NonExisting_Throws()
    {
        var missing = Path.Combine(_tempDir, "missing.json");

        Assert.Throws<FileNotFoundException>(() => RepositoryConfiguration.LoadFrom(missing));
    }

    [Fact]
    public void LoadFrom_MalformedJson_Throws()
    {
        var path = Path.Combine(_tempDir, "bad.json");
        File.WriteAllText(path, "{ this is not json");

        Assert.Throws<JsonException>(() => RepositoryConfiguration.LoadFrom(path));
    }

    [Fact]
    public void CreateRepositories_OnlyEnabledEntries()
    {
        var cfg = new RepositoryConfiguration();
        cfg.AddRepository(new RepositoryEntry { Name = "on", Source = "https://api.nuget.org/v3/index.json", Type = RepositoryType.NuGet, Enabled = true });
        cfg.AddRepository(new RepositoryEntry { Name = "off", Source = "https://api.nuget.org/v3/index.json", Type = RepositoryType.NuGet, Enabled = false });

        var repos = cfg.CreateRepositories().ToList();

        Assert.Single(repos);
    }

    [Fact]
    public void CreateRepositories_HonorsAllTypes()
    {
        var cfg = new RepositoryConfiguration();
        cfg.AddRepository(new RepositoryEntry { Name = "n", Source = "https://api.nuget.org/v3/index.json", Type = RepositoryType.NuGet });
        cfg.AddRepository(new RepositoryEntry { Name = "h", Source = "https://example.com", Type = RepositoryType.Http });
        cfg.AddRepository(new RepositoryEntry { Name = "m", Source = "https://marketplace.example.com", Type = RepositoryType.Marketplace });

        var repos = cfg.CreateRepositories().ToList();

        Assert.Equal(3, repos.Count);
    }

    [Fact]
    public void RepositoryEntry_Defaults_PackagePrefixIsNull_TypeIsNuGet_EnabledTrue()
    {
        var entry = new RepositoryEntry { Name = "x", Source = "https://example.com" };

        Assert.Equal(RepositoryType.NuGet, entry.Type);
        Assert.True(entry.Enabled);
        Assert.Null(entry.PackagePrefix);
        Assert.Null(entry.Credentials);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); }
            catch { /* best effort */ }
        }
    }
}

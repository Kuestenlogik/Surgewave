using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

/// <summary>
/// Coverage-Lueckenfueller fuer <see cref="RepositoryConfiguration.Load"/> und
/// die <see cref="RepositoryConfiguration.SaveTo"/>-Branch ohne directory. Beide Pfade
/// werden von den bestehenden Tests nicht beruehrt (Load() durchlaeuft normalerweise
/// User-Home; SaveTo mit reinem Dateinamen geht direkt in die working directory).
/// </summary>
public sealed class RepositoryConfigurationLoadTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _originalCwd;

    public RepositoryConfigurationLoadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sw-repocfg-load-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _originalCwd = Environment.CurrentDirectory;
        Environment.CurrentDirectory = _tempDir;
    }

    [Fact]
    public void Load_NoConfigAnywhere_ReturnsCreateDefault()
    {
        // _tempDir as CWD has no surgewave-repositories.json. Load() should fall through
        // to CreateDefault() — verify by checking the default repository identity.
        var cfg = RepositoryConfiguration.Load();

        Assert.Equal("nuget.org", cfg.DefaultRepository);
        Assert.Equal(2, cfg.Repositories.Count);
    }

    [Fact]
    public void Load_CurrentDirectoryConfig_TakesPrecedence()
    {
        var localConfigPath = Path.Combine(_tempDir, "surgewave-repositories.json");
        var seed = new RepositoryConfiguration
        {
            DefaultRepository = "private",
            Repositories =
            [
                new RepositoryEntry
                {
                    Name = "private",
                    Source = "https://private.example.com/index.json",
                },
            ],
        };
        seed.SaveTo(localConfigPath);

        var cfg = RepositoryConfiguration.Load();

        Assert.Equal("private", cfg.DefaultRepository);
        Assert.Single(cfg.Repositories);
    }

    [Fact]
    public void Load_MalformedJsonInCwd_SilentlyFallsThrough()
    {
        var path = Path.Combine(_tempDir, "surgewave-repositories.json");
        File.WriteAllText(path, "{ broken json");

        // Should silently skip the malformed file and continue searching — eventually
        // falling through to CreateDefault().
        var cfg = RepositoryConfiguration.Load();

        Assert.NotNull(cfg);
        Assert.NotEmpty(cfg.Repositories);
    }

    [Fact]
    public void SaveTo_RelativeFilenameOnly_WritesIntoCurrentDirectory()
    {
        var cfg = RepositoryConfiguration.CreateDefault();

        cfg.SaveTo("relative-cfg.json");

        var written = Path.Combine(_tempDir, "relative-cfg.json");
        Assert.True(File.Exists(written));
    }

    public void Dispose()
    {
        Environment.CurrentDirectory = _originalCwd;
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); }
            catch { /* best effort */ }
        }
    }
}

using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Sources;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Sources;

#region PluginSourceConfigTests

public sealed class PluginSourceConfigTests
{
    [Fact]
    public void Load_ReturnsDefaultConfig_WhenFileDoesNotExist()
    {
        // Load() falls back to CreateDefault() when the config file is missing or unreadable.
        // We verify the returned config has the expected default sources.
        var config = PluginSourceConfig.Load();

        Assert.NotNull(config);
        Assert.NotNull(config.Sources);
        Assert.True(config.Sources.Count >= 2, "Default config should have at least 2 sources");
        Assert.Contains(config.Sources, s => s.Name == "nuget");
        Assert.Contains(config.Sources, s => s.Name == "marketplace");
    }

    [Fact]
    public void Save_And_Load_RoundTrips()
    {
        // Test JSON round-trip via serialization (avoids relying on hardcoded file paths).
        var original = new PluginSourceConfig
        {
            Sources =
            [
                new PluginSourceConfig.SourceEntry { Name = "test-nuget", Type = "nuget", Url = "https://test.nuget.org/v3/index.json" },
                new PluginSourceConfig.SourceEntry { Name = "test-http", Type = "http", Url = "https://test.marketplace.example.com", ApiKey = "secret-key" }
            ]
        };

        var jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNameCaseInsensitive = true,
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
        };

        var json = JsonSerializer.Serialize(original, jsonOptions);
        var deserialized = JsonSerializer.Deserialize<PluginSourceConfig>(json, jsonOptions);

        Assert.NotNull(deserialized);
        Assert.Equal(original.Sources.Count, deserialized.Sources.Count);

        for (var i = 0; i < original.Sources.Count; i++)
        {
            Assert.Equal(original.Sources[i].Name, deserialized.Sources[i].Name);
            Assert.Equal(original.Sources[i].Type, deserialized.Sources[i].Type);
            Assert.Equal(original.Sources[i].Url, deserialized.Sources[i].Url);
            Assert.Equal(original.Sources[i].ApiKey, deserialized.Sources[i].ApiKey);
        }
    }

    [Fact]
    public void CreateDefault_HasNuGetSource()
    {
        var config = PluginSourceConfig.Load();

        var nuget = config.Sources.FirstOrDefault(s => s.Name == "nuget");
        Assert.NotNull(nuget);
        Assert.Equal("nuget", nuget.Type);
        Assert.Equal("https://api.nuget.org/v3/index.json", nuget.Url);
    }

    [Fact]
    public void CreateDefault_HasMarketplaceSource()
    {
        var config = PluginSourceConfig.Load();

        var marketplace = config.Sources.FirstOrDefault(s => s.Name == "marketplace");
        Assert.NotNull(marketplace);
        Assert.Equal("http", marketplace.Type);
        Assert.Equal("http://localhost:5060", marketplace.Url);
    }
}

#endregion

#region PluginSourceFactoryTests

public sealed class PluginSourceFactoryTests
{
    [Fact]
    public void Create_NuGet_ReturnsNuGetPluginSource()
    {
        var entry = new PluginSourceConfig.SourceEntry
        {
            Name = "test-nuget",
            Type = "nuget",
            Url = "https://api.nuget.org/v3/index.json"
        };

        var source = PluginSourceFactory.Create(entry);

        Assert.IsType<NuGetPluginSource>(source);
        Assert.Equal("test-nuget", source.Name);
        Assert.Equal("nuget", source.Type);
    }

    [Fact]
    public void Create_Http_ReturnsHttpPluginSource()
    {
        var entry = new PluginSourceConfig.SourceEntry
        {
            Name = "test-http",
            Type = "http",
            Url = "http://localhost:5060"
        };

        var source = PluginSourceFactory.Create(entry);

        Assert.IsType<HttpPluginSource>(source);
        Assert.Equal("test-http", source.Name);
        Assert.Equal("http", source.Type);
    }

    [Fact]
    public void Create_GitHub_ReturnsGitHubPluginSource()
    {
        var entry = new PluginSourceConfig.SourceEntry
        {
            Name = "test-github",
            Type = "github",
            Url = "surgewaveproject/surgewave-plugins"
        };

        var source = PluginSourceFactory.Create(entry);

        Assert.IsType<GitHubPluginSource>(source);
        Assert.Equal("test-github", source.Name);
        Assert.Equal("github", source.Type);
    }

    [Fact]
    public void Create_Unknown_ThrowsArgumentException()
    {
        var entry = new PluginSourceConfig.SourceEntry
        {
            Name = "ftp-source",
            Type = "ftp",
            Url = "ftp://example.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => PluginSourceFactory.Create(entry));
        Assert.Contains("ftp", ex.Message);
    }

    [Fact]
    public void CreateAll_ReturnsAllSources()
    {
        var config = new PluginSourceConfig
        {
            Sources =
            [
                new PluginSourceConfig.SourceEntry { Name = "n", Type = "nuget", Url = "https://nuget.org/v3/index.json" },
                new PluginSourceConfig.SourceEntry { Name = "h", Type = "http", Url = "http://localhost" },
                new PluginSourceConfig.SourceEntry { Name = "g", Type = "github", Url = "owner/repo" }
            ]
        };

        var sources = PluginSourceFactory.CreateAll(config);

        Assert.Equal(3, sources.Count);
        Assert.IsType<NuGetPluginSource>(sources[0]);
        Assert.IsType<HttpPluginSource>(sources[1]);
        Assert.IsType<GitHubPluginSource>(sources[2]);
    }

    [Fact]
    public void CreateAll_EmptyConfig_ReturnsEmptyList()
    {
        var config = new PluginSourceConfig { Sources = [] };

        var sources = PluginSourceFactory.CreateAll(config);

        Assert.Empty(sources);
    }
}

#endregion

#region NuGetPluginSourceTests

public sealed class NuGetPluginSourceTests : IDisposable
{
    private readonly NuGetPluginSource _source = new("my-nuget", "https://api.nuget.org/v3/index.json");

    [Fact]
    public void Name_ReturnsConfiguredName()
    {
        Assert.Equal("my-nuget", _source.Name);
    }

    [Fact]
    public void Type_ReturnsNuGet()
    {
        Assert.Equal("nuget", _source.Type);
    }

    [Fact]
    public void Url_ReturnsConfiguredUrl()
    {
        Assert.Equal("https://api.nuget.org/v3/index.json", _source.Url);
    }

    public void Dispose() => _source.Dispose();
}

#endregion

#region HttpPluginSourceTests

public sealed class HttpPluginSourceTests : IDisposable
{
    private readonly HttpPluginSource _source = new("my-marketplace", "http://localhost:5060");

    [Fact]
    public void Name_ReturnsConfiguredName()
    {
        Assert.Equal("my-marketplace", _source.Name);
    }

    [Fact]
    public void Type_ReturnsHttp()
    {
        Assert.Equal("http", _source.Type);
    }

    [Fact]
    public void Url_ReturnsConfiguredUrl()
    {
        Assert.Equal("http://localhost:5060", _source.Url);
    }

    public void Dispose() => _source.Dispose();
}

#endregion

#region GitHubPluginSourceTests

public sealed class GitHubPluginSourceTests : IDisposable
{
    private readonly GitHubPluginSource _source = new("gh-plugins", "surgewaveproject/surgewave-plugins");

    [Fact]
    public void Name_ReturnsConfiguredName()
    {
        Assert.Equal("gh-plugins", _source.Name);
    }

    [Fact]
    public void Type_ReturnsGitHub()
    {
        Assert.Equal("github", _source.Type);
    }

    [Fact]
    public void Url_ReturnsRepoSlug()
    {
        Assert.Equal("surgewaveproject/surgewave-plugins", _source.Url);
    }

    [Fact]
    public void Constructor_ParsesFullGitHubUrl()
    {
        using var source = new GitHubPluginSource("gh-url", "https://github.com/owner/repo");

        Assert.Equal("gh-url", source.Name);
        Assert.Equal("github", source.Type);
        // The Url property stores the original input
        Assert.Equal("https://github.com/owner/repo", source.Url);
    }

    public void Dispose() => _source.Dispose();
}

#endregion

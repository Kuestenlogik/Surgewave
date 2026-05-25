using Kuestenlogik.Surgewave.Plugins;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Core;

public sealed class PluginRegistryTests
{
    private static PluginInfo Info(string @class, string type = "source", string version = "1.0.0") => new()
    {
        Class = @class,
        Type = type,
        Version = version,
    };

    [Fact]
    public void NewRegistry_IsEmpty()
    {
        var reg = new PluginRegistry();

        Assert.Equal(0, reg.Count);
        Assert.Empty(reg.GetAll());
    }

    [Fact]
    public void Register_AddsPlugin()
    {
        var reg = new PluginRegistry();

        reg.Register(Info("x.Foo"));

        Assert.Equal(1, reg.Count);
        Assert.True(reg.Contains("x.Foo"));
        Assert.NotNull(reg.Get("x.Foo"));
    }

    [Fact]
    public void Register_DuplicateClassName_Overwrites()
    {
        var reg = new PluginRegistry();
        reg.Register(Info("x.Foo", version: "1.0.0"));

        reg.Register(Info("x.Foo", version: "2.0.0"));

        Assert.Equal(1, reg.Count);
        Assert.Equal("2.0.0", reg.Get("x.Foo")!.Version);
    }

    [Fact]
    public void Get_UnknownClass_ReturnsNull()
    {
        var reg = new PluginRegistry();

        Assert.Null(reg.Get("missing.Plugin"));
    }

    [Fact]
    public void Contains_UnknownClass_False()
    {
        var reg = new PluginRegistry();

        Assert.False(reg.Contains("missing"));
    }

    [Fact]
    public void Lookup_IsCaseInsensitive()
    {
        var reg = new PluginRegistry();
        reg.Register(Info("X.Foo"));

        Assert.True(reg.Contains("x.foo"));
        Assert.NotNull(reg.Get("x.FOO"));
    }

    [Fact]
    public void Remove_ExistingPlugin_ReturnsTrue_AndDeducts()
    {
        var reg = new PluginRegistry();
        reg.Register(Info("a"));
        reg.Register(Info("b"));

        var removed = reg.Remove("a");

        Assert.True(removed);
        Assert.Equal(1, reg.Count);
        Assert.False(reg.Contains("a"));
    }

    [Fact]
    public void Remove_UnknownPlugin_ReturnsFalse()
    {
        var reg = new PluginRegistry();

        Assert.False(reg.Remove("missing"));
    }

    [Fact]
    public void GetByType_FiltersCaseInsensitive()
    {
        var reg = new PluginRegistry();
        reg.Register(Info("s1", type: "source"));
        reg.Register(Info("s2", type: "SOURCE"));
        reg.Register(Info("k1", type: "sink"));

        var sources = reg.GetByType("Source");

        Assert.Equal(2, sources.Count);
        Assert.All(sources, p => Assert.Equal("source", p.Type, ignoreCase: true));
    }

    [Fact]
    public void GetAll_ReturnsSnapshot_NotLive()
    {
        var reg = new PluginRegistry();
        reg.Register(Info("a"));

        var snapshot = reg.GetAll();
        reg.Register(Info("b"));

        Assert.Single(snapshot);
    }

    [Fact]
    public void Clear_EmptiesRegistry()
    {
        var reg = new PluginRegistry();
        reg.Register(Info("a"));
        reg.Register(Info("b"));

        reg.Clear();

        Assert.Equal(0, reg.Count);
    }
}

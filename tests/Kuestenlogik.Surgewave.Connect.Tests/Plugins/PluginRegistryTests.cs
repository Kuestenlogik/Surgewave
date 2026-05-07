using Kuestenlogik.Surgewave.Plugins;

namespace Kuestenlogik.Surgewave.Connect.Tests.Plugins;

public class PluginRegistryTests
{
    [Fact]
    public void Register_AddsPlugin()
    {
        var registry = new PluginRegistry();
        var plugin = new PluginInfo { Class = "Test.Source", Type = "source", Version = "1.0.0" };

        registry.Register(plugin);

        Assert.Equal(1, registry.Count);
        Assert.True(registry.Contains("Test.Source"));
    }

    [Fact]
    public void Get_ReturnsRegisteredPlugin()
    {
        var registry = new PluginRegistry();
        var plugin = new PluginInfo { Class = "Test.Sink", Type = "sink", Version = "1.0.0", DisplayName = "Test Sink" };
        registry.Register(plugin);

        var result = registry.Get("Test.Sink");

        Assert.NotNull(result);
        Assert.Equal("Test Sink", result.DisplayName);
    }

    [Fact]
    public void Get_IsCaseInsensitive()
    {
        var registry = new PluginRegistry();
        registry.Register(new PluginInfo { Class = "Test.Plugin", Type = "source", Version = "1.0.0" });

        Assert.NotNull(registry.Get("test.plugin"));
        Assert.NotNull(registry.Get("TEST.PLUGIN"));
    }

    [Fact]
    public void Remove_DeletesPlugin()
    {
        var registry = new PluginRegistry();
        registry.Register(new PluginInfo { Class = "Test.Remove", Type = "sink", Version = "1.0.0" });

        Assert.True(registry.Remove("Test.Remove"));
        Assert.Equal(0, registry.Count);
    }

    [Fact]
    public void GetByType_FiltersCorrectly()
    {
        var registry = new PluginRegistry();
        registry.Register(new PluginInfo { Class = "A.Source", Type = "source", Version = "1.0.0" });
        registry.Register(new PluginInfo { Class = "B.Sink", Type = "sink", Version = "1.0.0" });
        registry.Register(new PluginInfo { Class = "C.Source", Type = "source", Version = "1.0.0" });

        var sources = registry.GetByType("source");

        Assert.Equal(2, sources.Count);
    }

    [Fact]
    public void Register_OverwritesDuplicate()
    {
        var registry = new PluginRegistry();
        registry.Register(new PluginInfo { Class = "Test.Plugin", Type = "source", Version = "1.0.0" });
        registry.Register(new PluginInfo { Class = "Test.Plugin", Type = "source", Version = "2.0.0" });

        Assert.Equal(1, registry.Count);
        Assert.Equal("2.0.0", registry.Get("Test.Plugin")!.Version);
    }

    [Fact]
    public void Clear_RemovesAll()
    {
        var registry = new PluginRegistry();
        registry.Register(new PluginInfo { Class = "A", Type = "source", Version = "1.0.0" });
        registry.Register(new PluginInfo { Class = "B", Type = "sink", Version = "1.0.0" });

        registry.Clear();

        Assert.Equal(0, registry.Count);
    }
}

using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kuestenlogik.Surgewave.Samples.StorageEngine.Tests;

public sealed class SampleMemoryStorageEnginePluginTests
{
    [Fact]
    public void Plugin_class_is_sealed()
    {
        Assert.True(typeof(SampleMemoryStorageEnginePlugin).IsSealed);
    }

    [Fact]
    public void StorageEngineName_is_in_SupportedModes()
    {
        var plugin = new SampleMemoryStorageEnginePlugin();
        Assert.Contains(plugin.StorageEngineName, plugin.SupportedModes);
    }

    [Fact]
    public void CreateFactory_returns_a_non_null_factory()
    {
        var plugin = new SampleMemoryStorageEnginePlugin();
        var config = new ConfigurationBuilder().Build();

        var factory = plugin.CreateFactory(plugin.StorageEngineName, config);

        Assert.NotNull(factory);
    }

    [Fact]
    public void Factory_marks_itself_as_non_persistent()
    {
        // The whole reason this engine exists — verify the in-memory
        // semantics survive the indirection through the plugin.
        var plugin = new SampleMemoryStorageEnginePlugin();
        var factory = plugin.CreateFactory(plugin.StorageEngineName, new ConfigurationBuilder().Build());

        Assert.False(factory.IsPersistent);
    }
}

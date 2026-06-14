using Xunit;

namespace MyEngine.Tests;

public sealed class MyEngineStoragePluginTests
{
    [Fact]
    public void StorageEngineName_is_in_SupportedModes()
    {
        var plugin = new MyEngineStoragePlugin();
        Assert.Contains(plugin.StorageEngineName, plugin.SupportedModes);
    }

    [Fact]
    public void FeatureId_is_set()
    {
        var plugin = new MyEngineStoragePlugin();
        Assert.False(string.IsNullOrEmpty(plugin.FeatureId));
    }
}

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MyPlugin.Tests;

public sealed class MyPluginBrokerPluginTests
{
    [Fact]
    public void FeatureId_is_set()
    {
        var plugin = new MyPluginBrokerPlugin();
        Assert.False(string.IsNullOrEmpty(plugin.FeatureId));
    }

    [Fact]
    public void IsConfigEnabled_defaults_to_true_when_unset()
    {
        var plugin = new MyPluginBrokerPlugin();
        var config = new ConfigurationBuilder().AddInMemoryCollection().Build();

        Assert.True(plugin.IsConfigEnabled(config));
    }

    [Fact]
    public void IsConfigEnabled_respects_MyPlugin_Enabled_false()
    {
        var plugin = new MyPluginBrokerPlugin();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new[] { new KeyValuePair<string, string?>("MyPlugin:Enabled", "false") })
            .Build();

        Assert.False(plugin.IsConfigEnabled(config));
    }

    [Fact]
    public void ConfigureServices_does_not_throw()
    {
        var plugin = new MyPluginBrokerPlugin();
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();

        plugin.ConfigureServices(services, config);
    }
}

using Microsoft.Extensions.Configuration;
using Xunit;

namespace MyProtocol.Tests;

public sealed class MyProtocolPluginTests
{
    [Fact]
    public void DefaultPort_is_zero_or_positive()
    {
        var plugin = new MyProtocolPlugin();
        Assert.True(plugin.DefaultPort >= 0);
    }

    [Fact]
    public void IsConfigEnabled_defaults_to_true_when_unset()
    {
        var plugin = new MyProtocolPlugin();
        var config = new ConfigurationBuilder().AddInMemoryCollection().Build();

        Assert.True(plugin.IsConfigEnabled(config));
    }

    [Fact]
    public void IsConfigEnabled_respects_disabled_flag()
    {
        var plugin = new MyProtocolPlugin();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new[] { new KeyValuePair<string, string?>("MyProtocol:Enabled", "false") })
            .Build();

        Assert.False(plugin.IsConfigEnabled(config));
    }
}

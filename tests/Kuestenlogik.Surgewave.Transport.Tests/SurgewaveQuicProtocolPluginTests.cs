using Kuestenlogik.Surgewave.Protocol.Quic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

public class SurgewaveQuicProtocolPluginTests
{
    [Fact]
    public void IsConfigEnabled_QuicEnabled_ReturnsTrue()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:Quic:Enabled"] = "true"
            })
            .Build();

        var plugin = new SurgewaveQuicProtocolPlugin();
        Assert.True(plugin.IsConfigEnabled(config));
    }

    [Fact]
    public void IsConfigEnabled_QuicDisabled_ReturnsFalse()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:Quic:Enabled"] = "false"
            })
            .Build();

        var plugin = new SurgewaveQuicProtocolPlugin();
        Assert.False(plugin.IsConfigEnabled(config));
    }

    [Fact]
    public void IsConfigEnabled_NoConfig_ReturnsFalse()
    {
        var config = new ConfigurationBuilder().Build();
        var plugin = new SurgewaveQuicProtocolPlugin();
        Assert.False(plugin.IsConfigEnabled(config));
    }

    [Fact]
    public void FeatureId_IsCorrect()
    {
        var plugin = new SurgewaveQuicProtocolPlugin();
        Assert.Equal("Surgewave.Protocol.Quic", plugin.FeatureId);
    }

    [Fact]
    public void DefaultPort_Is9094()
    {
        var plugin = new SurgewaveQuicProtocolPlugin();
        Assert.Equal(9094, plugin.DefaultPort);
    }

    [Fact]
    public void ConfigureServices_RegistersQuicConfig()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:Quic:Enabled"] = "true",
                ["Surgewave:Quic:Port"] = "9999"
            })
            .Build();

        var services = new ServiceCollection();
        var plugin = new SurgewaveQuicProtocolPlugin();
        plugin.ConfigureServices(services, config);

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<QuicConfig>>();
        Assert.Equal(9999, options.Value.Port);
    }
}

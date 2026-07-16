using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt.Tests;

/// <summary>
/// Pins the plugin surface of <see cref="SurgewaveMqttProtocolPlugin"/>: identity values,
/// config-gated enablement, and that ConfigureServices binds the Surgewave:Mqtt section and
/// registers the adapter as a hosted service.
/// </summary>
public sealed class SurgewaveMqttProtocolPluginTests : IDisposable
{
    private readonly LogManager _logManager = TestLogManager.CreateInMemory();

    public void Dispose() => _logManager.Dispose();

    [Fact]
    public void PluginIdentity_IsStable()
    {
        var plugin = new SurgewaveMqttProtocolPlugin();

        Assert.Equal("Surgewave.Protocol.Mqtt", plugin.FeatureId);
        Assert.Equal("MQTT Protocol", plugin.DisplayName);
        Assert.Equal(1883, plugin.DefaultPort);
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void IsConfigEnabled_ReadsEnabledFlag(string value, bool expected)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:Mqtt:Enabled"] = value,
            })
            .Build();

        Assert.Equal(expected, new SurgewaveMqttProtocolPlugin().IsConfigEnabled(configuration));
    }

    [Fact]
    public void IsConfigEnabled_MissingSection_DefaultsToFalse()
    {
        var configuration = new ConfigurationBuilder().Build();

        Assert.False(new SurgewaveMqttProtocolPlugin().IsConfigEnabled(configuration));
    }

    [Fact]
    public void ConfigureServices_BindsConfigSectionAndRegistersHostedAdapter()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:Mqtt:Port"] = "2883",
                ["Surgewave:Mqtt:TopicPrefix"] = "iot.",
                ["Surgewave:Mqtt:MaxClients"] = "5",
                ["Surgewave:Mqtt:AllowAnonymous"] = "false",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_logManager);
        new SurgewaveMqttProtocolPlugin().ConfigureServices(services, configuration);

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<MqttConfig>>().Value;

        Assert.Equal(2883, options.Port);
        Assert.Equal("iot.", options.TopicPrefix);
        Assert.Equal(5, options.MaxClients);
        Assert.False(options.AllowAnonymous);

        var hostedServices = provider.GetServices<IHostedService>();
        Assert.Contains(hostedServices, service => service is MqttProtocolAdapter);
    }
}

using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Pins the plugin surface of <see cref="SurgewaveWebSocketProtocolPlugin"/>: identity values,
/// config-gated enablement, DI registration/binding via <see cref="WebSocketServiceExtensions"/>,
/// and that Configure ignores hosts that are not a WebApplication.
/// </summary>
public sealed class SurgewaveWebSocketProtocolPluginTests : IDisposable
{
    private readonly LogManager _logManager = WebSocketAdapterTestHost.CreateInMemoryLogManager();

    public void Dispose() => _logManager.Dispose();

    [Fact]
    public void PluginIdentity_IsStable()
    {
        var plugin = new SurgewaveWebSocketProtocolPlugin();

        Assert.Equal("Surgewave.Protocol.WebSocket", plugin.FeatureId);
        Assert.Equal("WebSocket Protocol", plugin.DisplayName);
        Assert.Equal(0, plugin.DefaultPort);
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void IsConfigEnabled_ReadsEnabledFlag(string value, bool expected)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:WebSocket:Enabled"] = value,
            })
            .Build();

        Assert.Equal(expected, new SurgewaveWebSocketProtocolPlugin().IsConfigEnabled(configuration));
    }

    [Fact]
    public void IsConfigEnabled_MissingSection_DefaultsToFalse()
    {
        var configuration = new ConfigurationBuilder().Build();

        Assert.False(new SurgewaveWebSocketProtocolPlugin().IsConfigEnabled(configuration));
    }

    [Fact]
    public void ConfigureServices_BindsConfigSectionAndRegistersAdapter()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:WebSocket:Enabled"] = "true",
                ["Surgewave:WebSocket:Path"] = "/stream",
                ["Surgewave:WebSocket:MaxConnections"] = "7",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_logManager);
        new SurgewaveWebSocketProtocolPlugin().ConfigureServices(services, configuration);

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<WebSocketConfig>>().Value;

        Assert.True(options.Enabled);
        Assert.Equal("/stream", options.Path);
        Assert.Equal(7, options.MaxConnections);
        Assert.NotNull(provider.GetRequiredService<WebSocketProtocolAdapter>());
    }

    [Fact]
    public void Configure_NonWebApplicationHost_IsIgnored()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();

        var exception = Record.Exception(() => new SurgewaveWebSocketProtocolPlugin().Configure(new object(), provider));

        Assert.Null(exception);
    }

    [Fact]
    public void MapSurgewaveWebSocket_ResolvesAdapterAndMapsThreeEndpoints()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:WebSocket:Path"] = "/ws",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_logManager);
        services.AddSurgewaveWebSocket(configuration);
        using var provider = services.BuildServiceProvider();

        var builder = new TestEndpointRouteBuilder(provider);
        builder.MapSurgewaveWebSocket();

        Assert.Equal(3, builder.DataSources.SelectMany(source => source.Endpoints).Count());
    }
}

using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Tests for WebSocketConfig defaults and section name
/// </summary>
public sealed class WebSocketConfigTests
{
    [Fact]
    public void SectionName_IsCorrect()
    {
        Assert.Equal("Surgewave:WebSocket", WebSocketConfig.SectionName);
    }

    [Fact]
    public void DefaultEnabled_IsFalse()
    {
        var config = new WebSocketConfig();
        Assert.False(config.Enabled);
    }

    [Fact]
    public void DefaultPath_IsSlashWs()
    {
        var config = new WebSocketConfig();
        Assert.Equal("/ws", config.Path);
    }

    [Fact]
    public void DefaultMaxMessageSizeBytes_Is1MB()
    {
        var config = new WebSocketConfig();
        Assert.Equal(1_048_576, config.MaxMessageSizeBytes);
    }

    [Fact]
    public void DefaultPingInterval_Is30Seconds()
    {
        var config = new WebSocketConfig();
        Assert.Equal(TimeSpan.FromSeconds(30), config.PingInterval);
    }

    [Fact]
    public void DefaultMaxConnections_Is5000()
    {
        var config = new WebSocketConfig();
        Assert.Equal(5000, config.MaxConnections);
    }

    [Fact]
    public void Config_PropertiesAreMutable()
    {
        var config = new WebSocketConfig
        {
            Enabled = true,
            Path = "/websocket",
            MaxMessageSizeBytes = 512 * 1024,
            PingInterval = TimeSpan.FromSeconds(60),
            MaxConnections = 2000
        };

        Assert.True(config.Enabled);
        Assert.Equal("/websocket", config.Path);
        Assert.Equal(512 * 1024, config.MaxMessageSizeBytes);
        Assert.Equal(TimeSpan.FromSeconds(60), config.PingInterval);
        Assert.Equal(2000, config.MaxConnections);
    }
}

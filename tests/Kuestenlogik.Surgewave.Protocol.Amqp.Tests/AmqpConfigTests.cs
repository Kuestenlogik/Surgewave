using Kuestenlogik.Surgewave.Protocol.Amqp;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Amqp.Tests;

/// <summary>
/// Tests for AmqpConfig defaults and section name.
/// </summary>
public sealed class AmqpConfigTests
{
    [Fact]
    public void SectionName_IsCorrect()
    {
        Assert.Equal("Surgewave:Amqp", AmqpConfig.SectionName);
    }

    [Fact]
    public void DefaultEnabled_IsFalse()
    {
        var config = new AmqpConfig();
        Assert.False(config.Enabled);
    }

    [Fact]
    public void DefaultPort_Is5672()
    {
        var config = new AmqpConfig();
        Assert.Equal(5672, config.Port);
    }

    [Fact]
    public void DefaultMaxChannels_Is256()
    {
        var config = new AmqpConfig();
        Assert.Equal(256, config.MaxChannels);
    }

    [Fact]
    public void DefaultHeartbeatInterval_Is60()
    {
        var config = new AmqpConfig();
        Assert.Equal(60, config.HeartbeatInterval);
    }

    [Fact]
    public void DefaultMaxConnections_Is1000()
    {
        var config = new AmqpConfig();
        Assert.Equal(1000, config.MaxConnections);
    }

    [Fact]
    public void DefaultMaxFrameSize_Is131072()
    {
        var config = new AmqpConfig();
        Assert.Equal(131_072, config.MaxFrameSize);
    }

    [Fact]
    public void DefaultAllowAnonymous_IsTrue()
    {
        var config = new AmqpConfig();
        Assert.True(config.AllowAnonymous);
    }

    [Fact]
    public void DefaultVirtualHost_IsSlash()
    {
        var config = new AmqpConfig();
        Assert.Equal("/", config.VirtualHost);
    }

    [Fact]
    public void Config_PropertiesAreMutable()
    {
        var config = new AmqpConfig
        {
            Enabled = true,
            Port = 5671,
            MaxChannels = 128,
            HeartbeatInterval = 30,
            MaxConnections = 500,
            MaxFrameSize = 65536,
            AllowAnonymous = false,
            VirtualHost = "myapp",
        };

        Assert.True(config.Enabled);
        Assert.Equal(5671, config.Port);
        Assert.Equal(128, config.MaxChannels);
        Assert.Equal(30, config.HeartbeatInterval);
        Assert.Equal(500, config.MaxConnections);
        Assert.Equal(65536, config.MaxFrameSize);
        Assert.False(config.AllowAnonymous);
        Assert.Equal("myapp", config.VirtualHost);
    }
}

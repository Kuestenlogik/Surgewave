using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt.Tests;

/// <summary>
/// Tests for MqttConfig defaults and section name
/// </summary>
public sealed class MqttConfigTests
{
    [Fact]
    public void SectionName_IsCorrect()
    {
        Assert.Equal("Surgewave:Mqtt", MqttConfig.SectionName);
    }

    [Fact]
    public void DefaultPort_Is1883()
    {
        var config = new MqttConfig();
        Assert.Equal(1883, config.Port);
    }

    [Fact]
    public void DefaultEnabled_IsFalse()
    {
        var config = new MqttConfig();
        Assert.False(config.Enabled);
    }

    [Fact]
    public void DefaultTopicPrefix_IsMqttDot()
    {
        var config = new MqttConfig();
        Assert.Equal("mqtt.", config.TopicPrefix);
    }

    [Fact]
    public void DefaultMaxClients_Is1000()
    {
        var config = new MqttConfig();
        Assert.Equal(1000, config.MaxClients);
    }

    [Fact]
    public void DefaultMaxMessageSizeBytes_Is262144()
    {
        var config = new MqttConfig();
        Assert.Equal(262_144, config.MaxMessageSizeBytes);
    }

    [Fact]
    public void DefaultAllowAnonymous_IsTrue()
    {
        var config = new MqttConfig();
        Assert.True(config.AllowAnonymous);
    }

    [Fact]
    public void DefaultKeepAliveSeconds_Is60()
    {
        var config = new MqttConfig();
        Assert.Equal(60, config.KeepAliveSeconds);
    }

    [Fact]
    public void Config_PropertiesAreMutable()
    {
        var config = new MqttConfig
        {
            Enabled = true,
            Port = 8883,
            TopicPrefix = "iot.",
            MaxClients = 500,
            MaxMessageSizeBytes = 1024,
            AllowAnonymous = false,
            KeepAliveSeconds = 120
        };

        Assert.True(config.Enabled);
        Assert.Equal(8883, config.Port);
        Assert.Equal("iot.", config.TopicPrefix);
        Assert.Equal(500, config.MaxClients);
        Assert.Equal(1024, config.MaxMessageSizeBytes);
        Assert.False(config.AllowAnonymous);
        Assert.Equal(120, config.KeepAliveSeconds);
    }
}

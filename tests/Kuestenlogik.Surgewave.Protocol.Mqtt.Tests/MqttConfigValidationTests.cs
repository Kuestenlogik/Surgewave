using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt.Tests;

/// <summary>
/// Pins the data-annotation validation rules of <see cref="MqttConfig.Validate"/>:
/// port range, required topic prefix, and positive client/size/keep-alive limits.
/// </summary>
public sealed class MqttConfigValidationTests
{
    [Fact]
    public void Validate_DefaultConfig_HasNoErrors()
    {
        var config = new MqttConfig();

        var errors = config.Validate();

        Assert.Empty(errors);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(65536)]
    public void Validate_PortOutOfRange_ReportsPortError(int port)
    {
        var config = new MqttConfig { Port = port };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("Port", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(65535)]
    public void Validate_PortAtRangeBounds_IsValid(int port)
    {
        var config = new MqttConfig { Port = port };

        var errors = config.Validate();

        Assert.Empty(errors);
    }

    [Fact]
    public void Validate_EmptyTopicPrefix_ReportsRequiredError()
    {
        var config = new MqttConfig { TopicPrefix = "" };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("TopicPrefix", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_ZeroMaxClients_ReportsRangeError()
    {
        var config = new MqttConfig { MaxClients = 0 };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("MaxClients", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_ZeroMaxMessageSize_ReportsRangeError()
    {
        var config = new MqttConfig { MaxMessageSizeBytes = 0 };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("MaxMessageSizeBytes", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_ZeroKeepAlive_ReportsRangeError()
    {
        var config = new MqttConfig { KeepAliveSeconds = 0 };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("KeepAliveSeconds", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_MultipleViolations_ReportsAllOfThem()
    {
        var config = new MqttConfig
        {
            Port = 0,
            TopicPrefix = "",
            KeepAliveSeconds = -1,
        };

        var errors = config.Validate();

        Assert.Equal(3, errors.Count);
    }
}

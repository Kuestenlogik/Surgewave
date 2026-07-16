using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Pins the validation rules of <see cref="WebSocketConfig.Validate"/>: data-annotation
/// constraints on Path, MaxMessageSizeBytes and MaxConnections plus the custom PingInterval rule.
/// </summary>
public sealed class WebSocketConfigValidationTests
{
    [Fact]
    public void Validate_DefaultConfig_HasNoErrors()
    {
        var config = new WebSocketConfig();

        var errors = config.Validate();

        Assert.Empty(errors);
    }

    [Fact]
    public void Validate_PathWithoutLeadingSlash_ReportsPathError()
    {
        var config = new WebSocketConfig { Path = "ws" };

        var errors = config.Validate();

        Assert.Contains(errors, e =>
            e.Contains("Path", StringComparison.Ordinal) &&
            e.Contains("must start with '/'", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_EmptyPath_ReportsRequiredError()
    {
        var config = new WebSocketConfig { Path = "" };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("Path", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_ZeroMaxMessageSize_ReportsRangeError()
    {
        var config = new WebSocketConfig { MaxMessageSizeBytes = 0 };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("MaxMessageSizeBytes", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_ZeroMaxConnections_ReportsRangeError()
    {
        var config = new WebSocketConfig { MaxConnections = 0 };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("MaxConnections", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void Validate_NonPositivePingInterval_ReportsCustomError(int seconds)
    {
        var config = new WebSocketConfig { PingInterval = TimeSpan.FromSeconds(seconds) };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains("PingInterval", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_MultipleViolations_ReportsAllOfThem()
    {
        var config = new WebSocketConfig
        {
            Path = "no-slash",
            MaxMessageSizeBytes = 0,
            MaxConnections = -1,
            PingInterval = TimeSpan.Zero,
        };

        var errors = config.Validate();

        Assert.Equal(4, errors.Count);
    }
}

using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for ConfigParser byte and time parsing utilities.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ConfigParserTests
{
    #region TryParseBytes Tests

    [Theory]
    [InlineData("1024", 1024)]
    [InlineData("1", 1)]
    [InlineData("1000000", 1000000)]
    public void TryParseBytes_PlainNumber_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseBytes(input, out var bytes);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, bytes);
    }

    [Theory]
    [InlineData("1KB", 1024)]
    [InlineData("1K", 1024)]
    [InlineData("10KB", 10240)]
    [InlineData("100KB", 102400)]
    public void TryParseBytes_Kilobytes_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseBytes(input, out var bytes);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, bytes);
    }

    [Theory]
    [InlineData("1MB", 1024 * 1024)]
    [InlineData("1M", 1024 * 1024)]
    [InlineData("10MB", 10 * 1024 * 1024)]
    [InlineData("100MB", 100 * 1024 * 1024)]
    public void TryParseBytes_Megabytes_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseBytes(input, out var bytes);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, bytes);
    }

    [Theory]
    [InlineData("1GB", 1024L * 1024 * 1024)]
    [InlineData("1G", 1024L * 1024 * 1024)]
    [InlineData("10GB", 10L * 1024 * 1024 * 1024)]
    public void TryParseBytes_Gigabytes_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseBytes(input, out var bytes);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, bytes);
    }

    [Theory]
    [InlineData("1TB", 1024L * 1024 * 1024 * 1024)]
    [InlineData("1T", 1024L * 1024 * 1024 * 1024)]
    public void TryParseBytes_Terabytes_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseBytes(input, out var bytes);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, bytes);
    }

    [Theory]
    [InlineData("1kb")]
    [InlineData("1Kb")]
    [InlineData("1kB")]
    [InlineData("1mb")]
    [InlineData("1gb")]
    public void TryParseBytes_CaseInsensitive_ParsesCorrectly(string input)
    {
        // Act
        var success = ConfigParser.TryParseBytes(input, out var bytes);

        // Assert
        Assert.True(success);
        Assert.True(bytes > 0);
    }

    [Theory]
    [InlineData("1.5MB")]
    [InlineData("2.5GB")]
    public void TryParseBytes_Decimal_ParsesCorrectly(string input)
    {
        // Act
        var success = ConfigParser.TryParseBytes(input, out var bytes);

        // Assert
        Assert.True(success);
        Assert.True(bytes > 0);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void TryParseBytes_EmptyOrNull_ReturnsFalse(string? input)
    {
        // Act
        var success = ConfigParser.TryParseBytes(input!, out var bytes);

        // Assert
        Assert.False(success);
        Assert.Equal(0, bytes);
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("0")]
    [InlineData("-1KB")]
    public void TryParseBytes_NegativeOrZero_ReturnsFalse(string input)
    {
        // Act
        var success = ConfigParser.TryParseBytes(input, out var bytes);

        // Assert
        Assert.False(success);
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("1XB")]
    public void TryParseBytes_InvalidFormat_ReturnsFalse(string input)
    {
        // Act
        var success = ConfigParser.TryParseBytes(input, out var bytes);

        // Assert
        Assert.False(success);
    }

    [Fact]
    public void TryParseBytes_WithWhitespace_ParsesCorrectly()
    {
        // The regex allows whitespace between number and unit
        // Act
        var success = ConfigParser.TryParseBytes("1 KB", out var bytes);

        // Assert
        Assert.True(success);
        Assert.Equal(1024, bytes);
    }

    #endregion

    #region TryParseMilliseconds Tests

    [Theory]
    [InlineData("1000", 1000)]
    [InlineData("0", 0)]
    [InlineData("60000", 60000)]
    public void TryParseMilliseconds_PlainNumber_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseMilliseconds(input, out var ms);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, ms);
    }

    [Theory]
    [InlineData("1s", 1000)]
    [InlineData("60s", 60000)]
    [InlineData("30s", 30000)]
    public void TryParseMilliseconds_Seconds_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseMilliseconds(input, out var ms);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, ms);
    }

    [Theory]
    [InlineData("1m", 60 * 1000)]
    [InlineData("30m", 30 * 60 * 1000)]
    [InlineData("60m", 60 * 60 * 1000)]
    public void TryParseMilliseconds_Minutes_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseMilliseconds(input, out var ms);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, ms);
    }

    [Theory]
    [InlineData("1h", 60 * 60 * 1000)]
    [InlineData("24h", 24 * 60 * 60 * 1000)]
    [InlineData("12h", 12 * 60 * 60 * 1000)]
    public void TryParseMilliseconds_Hours_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseMilliseconds(input, out var ms);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, ms);
    }

    [Theory]
    [InlineData("1d", 24L * 60 * 60 * 1000)]
    [InlineData("7d", 7L * 24 * 60 * 60 * 1000)]
    [InlineData("30d", 30L * 24 * 60 * 60 * 1000)]
    public void TryParseMilliseconds_Days_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseMilliseconds(input, out var ms);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, ms);
    }

    [Theory]
    [InlineData("1w", 7L * 24 * 60 * 60 * 1000)]
    [InlineData("2w", 14L * 24 * 60 * 60 * 1000)]
    public void TryParseMilliseconds_Weeks_ParsesCorrectly(string input, long expected)
    {
        // Act
        var success = ConfigParser.TryParseMilliseconds(input, out var ms);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, ms);
    }

    [Theory]
    [InlineData("1S")]
    [InlineData("1s")]
    [InlineData("1M")]
    [InlineData("1h")]
    [InlineData("1H")]
    public void TryParseMilliseconds_CaseInsensitive_ParsesCorrectly(string input)
    {
        // Act
        var success = ConfigParser.TryParseMilliseconds(input, out var ms);

        // Assert
        Assert.True(success);
        Assert.True(ms > 0);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void TryParseMilliseconds_EmptyOrWhitespace_ReturnsFalse(string input)
    {
        // Act
        var success = ConfigParser.TryParseMilliseconds(input, out _);

        // Assert
        Assert.False(success);
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("1x")]
    public void TryParseMilliseconds_InvalidFormat_ReturnsFalse(string input)
    {
        // Act
        var success = ConfigParser.TryParseMilliseconds(input, out _);

        // Assert
        Assert.False(success);
    }

    [Fact]
    public void TryParseMilliseconds_WithWhitespace_ParsesCorrectly()
    {
        // The regex allows whitespace between number and unit
        // Act
        var success = ConfigParser.TryParseMilliseconds("1 h", out var ms);

        // Assert
        Assert.True(success);
        Assert.Equal(60 * 60 * 1000, ms);
    }

    #endregion

    #region FormatBytes Tests

    [Theory]
    [InlineData(0, "0B")]
    [InlineData(512, "512B")]
    [InlineData(1023, "1023B")]
    public void FormatBytes_ByteRange_FormatsCorrectly(long bytes, string expected)
    {
        // Act
        var result = ConfigParser.FormatBytes(bytes);

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(1024, "1KB")]
    [InlineData(2048, "2KB")]
    public void FormatBytes_KilobyteRange_FormatsCorrectly(long bytes, string expected)
    {
        // Act
        var result = ConfigParser.FormatBytes(bytes);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void FormatBytes_KilobyteRange_Decimal_FormatsCorrectly()
    {
        // Act
        var result = ConfigParser.FormatBytes(1536);

        // Assert - decimal separator is culture-dependent
        Assert.True(result == "1.5KB" || result == "1,5KB");
        Assert.EndsWith("KB", result);
    }

    [Fact]
    public void FormatBytes_Megabytes_FormatsCorrectly()
    {
        // Act
        var result = ConfigParser.FormatBytes(1024 * 1024);

        // Assert
        Assert.Equal("1MB", result);
    }

    [Fact]
    public void FormatBytes_Gigabytes_FormatsCorrectly()
    {
        // Act
        var result = ConfigParser.FormatBytes(1024L * 1024 * 1024);

        // Assert
        Assert.Equal("1GB", result);
    }

    [Fact]
    public void FormatBytes_Terabytes_FormatsCorrectly()
    {
        // Act
        var result = ConfigParser.FormatBytes(1024L * 1024 * 1024 * 1024);

        // Assert
        Assert.Equal("1TB", result);
    }

    #endregion

    #region FormatMilliseconds Tests

    [Theory]
    [InlineData(0, "0ms")]
    [InlineData(500, "500ms")]
    [InlineData(999, "999ms")]
    public void FormatMilliseconds_MillisecondRange_FormatsCorrectly(long ms, string expected)
    {
        // Act
        var result = ConfigParser.FormatMilliseconds(ms);

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(1000, "1s")]
    [InlineData(5000, "5s")]
    [InlineData(30000, "30s")]
    public void FormatMilliseconds_SecondRange_FormatsCorrectly(long ms, string expected)
    {
        // Act
        var result = ConfigParser.FormatMilliseconds(ms);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void FormatMilliseconds_Minutes_FormatsCorrectly()
    {
        // Act
        var result = ConfigParser.FormatMilliseconds(60 * 1000);

        // Assert
        Assert.Equal("1m", result);
    }

    [Fact]
    public void FormatMilliseconds_Hours_FormatsCorrectly()
    {
        // Act
        var result = ConfigParser.FormatMilliseconds(60 * 60 * 1000);

        // Assert
        Assert.Equal("1h", result);
    }

    [Fact]
    public void FormatMilliseconds_Days_FormatsCorrectly()
    {
        // Act
        var result = ConfigParser.FormatMilliseconds(24L * 60 * 60 * 1000);

        // Assert
        Assert.Equal("1d", result);
    }

    #endregion

    #region GetSegmentBytes Tests

    [Fact]
    public void GetSegmentBytes_ShortName_ReturnsValue()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["segment"] = "100MB" };

        // Act
        var result = ConfigParser.GetSegmentBytes(config, 0);

        // Assert
        Assert.Equal(100 * 1024 * 1024, result);
    }

    [Fact]
    public void GetSegmentBytes_KafkaName_ReturnsValue()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["segment.bytes"] = "1073741824" };

        // Act
        var result = ConfigParser.GetSegmentBytes(config, 0);

        // Assert
        Assert.Equal(1073741824, result);
    }

    [Fact]
    public void GetSegmentBytes_ShortNameTakesPrecedence()
    {
        // Arrange
        var config = new Dictionary<string, string>
        {
            ["segment"] = "100MB",
            ["segment.bytes"] = "200000000"
        };

        // Act
        var result = ConfigParser.GetSegmentBytes(config, 0);

        // Assert
        Assert.Equal(100 * 1024 * 1024, result);
    }

    [Fact]
    public void GetSegmentBytes_NotFound_ReturnsDefault()
    {
        // Arrange
        var config = new Dictionary<string, string>();

        // Act
        var result = ConfigParser.GetSegmentBytes(config, 1234567);

        // Assert
        Assert.Equal(1234567, result);
    }

    #endregion

    #region GetRetentionMs Tests

    [Fact]
    public void GetRetentionMs_ShortName_ReturnsValue()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["retention"] = "7d" };

        // Act
        var result = ConfigParser.GetRetentionMs(config, 0);

        // Assert
        Assert.Equal(7L * 24 * 60 * 60 * 1000, result);
    }

    [Fact]
    public void GetRetentionMs_KafkaName_ReturnsValue()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["retention.ms"] = "604800000" };

        // Act
        var result = ConfigParser.GetRetentionMs(config, 0);

        // Assert
        Assert.Equal(604800000, result);
    }

    [Fact]
    public void GetRetentionMs_NotFound_ReturnsDefault()
    {
        // Arrange
        var config = new Dictionary<string, string>();

        // Act
        var result = ConfigParser.GetRetentionMs(config, 168 * 60 * 60 * 1000);

        // Assert
        Assert.Equal(168L * 60 * 60 * 1000, result);
    }

    #endregion

    #region GetMaxMessageBytes Tests

    [Fact]
    public void GetMaxMessageBytes_ShortName_ReturnsValue()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["max.message"] = "1MB" };

        // Act
        var result = ConfigParser.GetMaxMessageBytes(config, 0);

        // Assert
        Assert.Equal(1024 * 1024, result);
    }

    [Fact]
    public void GetMaxMessageBytes_KafkaName_ReturnsValue()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["max.message.bytes"] = "1048576" };

        // Act
        var result = ConfigParser.GetMaxMessageBytes(config, 0);

        // Assert
        Assert.Equal(1048576, result);
    }

    #endregion

    #region GetRetentionBytes Tests

    [Fact]
    public void GetRetentionBytes_ReturnsValue()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["retention.bytes"] = "10GB" };

        // Act
        var result = ConfigParser.GetRetentionBytes(config, -1);

        // Assert
        Assert.Equal(10L * 1024 * 1024 * 1024, result);
    }

    [Fact]
    public void GetRetentionBytes_NotFound_ReturnsDefault()
    {
        // Arrange
        var config = new Dictionary<string, string>();

        // Act
        var result = ConfigParser.GetRetentionBytes(config, -1);

        // Assert
        Assert.Equal(-1, result);
    }

    #endregion

    #region NormalizeConfig Tests

    [Fact]
    public void NormalizeConfig_ShortName_ConvertedToKafkaName()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["segment"] = "100MB" };

        // Act
        var normalized = ConfigParser.NormalizeConfig(config);

        // Assert
        Assert.Contains("segment.bytes", normalized.Keys);
        Assert.Equal((100 * 1024 * 1024).ToString(), normalized["segment.bytes"]);
    }

    [Fact]
    public void NormalizeConfig_TimeValue_ConvertedToMs()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["retention"] = "7d" };

        // Act
        var normalized = ConfigParser.NormalizeConfig(config);

        // Assert
        Assert.Contains("retention.ms", normalized.Keys);
        Assert.Equal((7L * 24 * 60 * 60 * 1000).ToString(), normalized["retention.ms"]);
    }

    [Fact]
    public void NormalizeConfig_UnknownKey_PassedThrough()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["unknown.key"] = "value" };

        // Act
        var normalized = ConfigParser.NormalizeConfig(config);

        // Assert
        Assert.Contains("unknown.key", normalized.Keys);
        Assert.Equal("value", normalized["unknown.key"]);
    }

    [Fact]
    public void NormalizeConfig_StringConfig_PassedThrough()
    {
        // Arrange
        var config = new Dictionary<string, string> { ["cleanup.policy"] = "compact" };

        // Act
        var normalized = ConfigParser.NormalizeConfig(config);

        // Assert
        Assert.Contains("cleanup.policy", normalized.Keys);
        Assert.Equal("compact", normalized["cleanup.policy"]);
    }

    #endregion
}

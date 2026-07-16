namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins the LogMessage callback payload and the SyslogLevel numeric values,
/// which must match RFC 5424 severities (and Confluent.Kafka) so that log
/// filtering in migrated applications keeps working.
/// </summary>
public class LogMessageTests
{
    [Fact]
    public void Constructor_SetsAllProperties()
    {
        var message = new LogMessage("producer-1", SyslogLevel.Warning, "broker", "connection lost");

        Assert.Equal("producer-1", message.Name);
        Assert.Equal(SyslogLevel.Warning, message.Level);
        Assert.Equal("broker", message.Facility);
        Assert.Equal("connection lost", message.Message);
    }

    [Theory]
    [InlineData(SyslogLevel.Emergency, 0)]
    [InlineData(SyslogLevel.Alert, 1)]
    [InlineData(SyslogLevel.Critical, 2)]
    [InlineData(SyslogLevel.Error, 3)]
    [InlineData(SyslogLevel.Warning, 4)]
    [InlineData(SyslogLevel.Notice, 5)]
    [InlineData(SyslogLevel.Info, 6)]
    [InlineData(SyslogLevel.Debug, 7)]
    public void SyslogLevel_MatchesRfc5424Severity(SyslogLevel level, int expected)
    {
        Assert.Equal(expected, (int)level);
    }
}

namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins Timestamp conversion semantics: unix-millisecond construction from
/// long/DateTime/DateTimeOffset, UTC round-trips, value-plus-type equality,
/// and the Default (NotAvailable) sentinel.
/// </summary>
public class TimestampTests
{
    [Fact]
    public void Constructor_FromUnixMs_DefaultsToCreateTime()
    {
        var ts = new Timestamp(1234567890L);

        Assert.Equal(1234567890L, ts.UnixTimestampMs);
        Assert.Equal(TimestampType.CreateTime, ts.Type);
    }

    [Fact]
    public void Constructor_WithExplicitType_SetsType()
    {
        var ts = new Timestamp(100, TimestampType.LogAppendTime);
        Assert.Equal(TimestampType.LogAppendTime, ts.Type);
    }

    [Fact]
    public void Default_IsZeroAndNotAvailable()
    {
        Assert.Equal(0, Timestamp.Default.UnixTimestampMs);
        Assert.Equal(TimestampType.NotAvailable, Timestamp.Default.Type);
    }

    [Fact]
    public void Constructor_FromUtcDateTime_ComputesUnixMs()
    {
        var utc = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        var ts = new Timestamp(utc);

        Assert.Equal(1705320000000L, ts.UnixTimestampMs);
    }

    [Fact]
    public void Constructor_FromLocalDateTime_ConvertsToUtc()
    {
        var local = new DateTime(2024, 6, 1, 8, 30, 0, DateTimeKind.Local);

        var ts = new Timestamp(local);

        Assert.Equal(new DateTimeOffset(local).ToUnixTimeMilliseconds(), ts.UnixTimestampMs);
    }

    [Fact]
    public void Constructor_FromDateTimeOffset_UsesOffset()
    {
        var dto = new DateTimeOffset(2024, 1, 15, 12, 0, 0, TimeSpan.FromHours(2));

        var ts = new Timestamp(dto);

        Assert.Equal(1705312800000L, ts.UnixTimestampMs);
    }

    [Fact]
    public void UtcDateTime_RoundTripsAndIsUtcKind()
    {
        var utc = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        var ts = new Timestamp(utc);

        Assert.Equal(utc, ts.UtcDateTime);
        Assert.Equal(DateTimeKind.Utc, ts.UtcDateTime.Kind);
    }

    [Fact]
    public void DateTimeOffset_RoundTripsUnixMs()
    {
        var ts = new Timestamp(1705312800000L);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1705312800000L), ts.DateTimeOffset);
    }

    [Fact]
    public void Equality_SameValueAndType_ReturnsTrue()
    {
        var a = new Timestamp(100, TimestampType.CreateTime);
        var b = new Timestamp(100, TimestampType.CreateTime);

        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void Equality_DifferentType_ReturnsFalse()
    {
        var create = new Timestamp(100, TimestampType.CreateTime);
        var append = new Timestamp(100, TimestampType.LogAppendTime);

        Assert.NotEqual(create, append);
        Assert.True(create != append);
    }

    [Fact]
    public void Equality_DifferentValue_ReturnsFalse()
    {
        var a = new Timestamp(100);
        var b = new Timestamp(101);

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void ToString_FormatsAsIso8601WithType()
    {
        var ts = new Timestamp(0);
        Assert.Equal("1970-01-01T00:00:00.0000000Z (CreateTime)", ts.ToString());
    }

    [Theory]
    [InlineData(TimestampType.NotAvailable, 0)]
    [InlineData(TimestampType.CreateTime, 1)]
    [InlineData(TimestampType.LogAppendTime, 2)]
    public void TimestampType_MatchesKafkaProtocolValue(TimestampType type, int expected)
    {
        Assert.Equal(expected, (int)type);
    }
}

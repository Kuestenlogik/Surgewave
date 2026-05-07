using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Tests for CdcSourceStatus record and CdcSourceState enum.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CdcSourceStatusTests
{
    [Fact]
    public void CdcSourceStatus_AllProperties_AreSet()
    {
        var startedAt = new DateTimeOffset(2026, 3, 15, 10, 0, 0, TimeSpan.Zero);
        var lastEventTs = new DateTimeOffset(2026, 3, 15, 12, 30, 0, TimeSpan.Zero);

        var status = new CdcSourceStatus
        {
            Id = "source-1",
            DatabaseType = "PostgreSQL",
            State = CdcSourceState.Streaming,
            SlotName = "my_slot",
            PublicationName = "my_pub",
            TrackedTables = 5,
            EventsCaptured = 1000,
            LastLsn = 999888777,
            LastEventTimestamp = lastEventTs,
            Error = null,
            StartedAt = startedAt
        };

        Assert.Equal("source-1", status.Id);
        Assert.Equal("PostgreSQL", status.DatabaseType);
        Assert.Equal(CdcSourceState.Streaming, status.State);
        Assert.Equal("my_slot", status.SlotName);
        Assert.Equal("my_pub", status.PublicationName);
        Assert.Equal(5, status.TrackedTables);
        Assert.Equal(1000, status.EventsCaptured);
        Assert.Equal(999888777, status.LastLsn);
        Assert.Equal(lastEventTs, status.LastEventTimestamp);
        Assert.Null(status.Error);
        Assert.Equal(startedAt, status.StartedAt);
    }

    [Fact]
    public void CdcSourceStatus_FaultedState_IncludesError()
    {
        var status = new CdcSourceStatus
        {
            Id = "faulted-source",
            DatabaseType = "PostgreSQL",
            State = CdcSourceState.Faulted,
            SlotName = "slot",
            PublicationName = "pub",
            Error = "Connection refused"
        };

        Assert.Equal(CdcSourceState.Faulted, status.State);
        Assert.Equal("Connection refused", status.Error);
    }

    [Fact]
    public void CdcSourceStatus_DefaultOptionalValues()
    {
        var status = new CdcSourceStatus
        {
            Id = "default-test",
            DatabaseType = "PostgreSQL",
            State = CdcSourceState.Initializing,
            SlotName = "slot",
            PublicationName = "pub"
        };

        Assert.Equal(0, status.TrackedTables);
        Assert.Equal(0, status.EventsCaptured);
        Assert.Equal(0, status.LastLsn);
        Assert.Null(status.LastEventTimestamp);
        Assert.Null(status.Error);
        Assert.Equal(default, status.StartedAt);
    }

    [Fact]
    public void CdcSourceStatus_RecordEquality()
    {
        var ts = DateTimeOffset.UtcNow;
        var status1 = new CdcSourceStatus
        {
            Id = "eq-test",
            DatabaseType = "PostgreSQL",
            State = CdcSourceState.Streaming,
            SlotName = "s",
            PublicationName = "p",
            StartedAt = ts
        };
        var status2 = new CdcSourceStatus
        {
            Id = "eq-test",
            DatabaseType = "PostgreSQL",
            State = CdcSourceState.Streaming,
            SlotName = "s",
            PublicationName = "p",
            StartedAt = ts
        };

        Assert.Equal(status1, status2);
    }

    [Fact]
    public void CdcSourceState_AllValues_Exist()
    {
        var values = Enum.GetValues<CdcSourceState>();

        Assert.Contains(CdcSourceState.Initializing, values);
        Assert.Contains(CdcSourceState.Snapshotting, values);
        Assert.Contains(CdcSourceState.Streaming, values);
        Assert.Contains(CdcSourceState.Stopped, values);
        Assert.Contains(CdcSourceState.Faulted, values);
        Assert.Equal(5, values.Length);
    }

    [Theory]
    [InlineData(CdcSourceState.Initializing, "Initializing")]
    [InlineData(CdcSourceState.Snapshotting, "Snapshotting")]
    [InlineData(CdcSourceState.Streaming, "Streaming")]
    [InlineData(CdcSourceState.Stopped, "Stopped")]
    [InlineData(CdcSourceState.Faulted, "Faulted")]
    public void CdcSourceState_ToString_ReturnsExpected(CdcSourceState state, string expected)
    {
        Assert.Equal(expected, state.ToString());
    }
}

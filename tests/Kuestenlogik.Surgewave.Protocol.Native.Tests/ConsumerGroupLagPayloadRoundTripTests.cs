using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — group-lag admin RPCs. The Control UI's "Groups"
/// tab and the <c>surgewave group lag ...</c> CLI both round-trip these
/// payloads on every refresh, so a framing regression here surfaces as
/// "always shows 0 lag" or "missing groups in summary" — exactly the
/// kind of admin-side bug that's annoying to root-cause from a customer
/// report without a wire-shape pin.
///
/// Covers <see cref="GetGroupLagRequestPayload"/>,
/// <see cref="GetGroupLagResponsePayload"/> (with nested
/// <see cref="TopicLagPayload"/> + <see cref="PartitionLagPayload"/>),
/// and <see cref="GetLagSummaryResponsePayload"/> (with nested
/// <see cref="LagSummaryGroupPayload"/>).
/// </summary>
public sealed class ConsumerGroupLagPayloadRoundTripTests
{
    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // GetGroupLag
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void GetGroupLagRequest_RoundTrip_PreservesGroupId()
    {
        var original = new GetGroupLagRequestPayload { GroupId = "g-analytics" };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetGroupLagRequestPayload.Read(ref r); });
        Assert.Equal("g-analytics", parsed.GroupId);
    }

    [Fact]
    public void GetGroupLagResponse_RoundTrip_PreservesAggregatesAndPerPartitionLag()
    {
        var original = new GetGroupLagResponsePayload
        {
            ErrorCode = 0,
            GroupId = "g-analytics",
            State = "Stable",
            TotalLag = 1_500_000L,
            PartitionCount = 64,
            MemberCount = 4,
            Topics = new[]
            {
                new TopicLagPayload
                {
                    Topic = "events",
                    TotalLag = 1_000_000L,
                    Partitions = new[]
                    {
                        new PartitionLagPayload { Partition = 0, CommittedOffset = 1_000, HighWatermark = 251_000, Lag = 250_000, LogStartOffset = 0 },
                        new PartitionLagPayload { Partition = 1, CommittedOffset = 5_000, HighWatermark = 255_000, Lag = 250_000, LogStartOffset = 0 },
                    },
                },
                new TopicLagPayload
                {
                    Topic = "audit",
                    TotalLag = 500_000L,
                    Partitions = new[]
                    {
                        new PartitionLagPayload { Partition = 0, CommittedOffset = 100, HighWatermark = 500_100, Lag = 500_000, LogStartOffset = 50 },
                    },
                },
            },
        };

        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetGroupLagResponsePayload.Read(ref r); });

        Assert.Equal(1_500_000L, parsed.TotalLag);
        Assert.Equal(64, parsed.PartitionCount);
        Assert.Equal(4, parsed.MemberCount);
        Assert.Equal(2, parsed.Topics.Length);

        // Topic 0
        Assert.Equal("events", parsed.Topics[0].Topic);
        Assert.Equal(1_000_000L, parsed.Topics[0].TotalLag);
        Assert.Equal(2, parsed.Topics[0].Partitions.Length);
        Assert.Equal(250_000L, parsed.Topics[0].Partitions[0].Lag);
        Assert.Equal(255_000L, parsed.Topics[0].Partitions[1].HighWatermark);

        // Topic 1
        Assert.Equal("audit", parsed.Topics[1].Topic);
        Assert.Equal(500_000L, parsed.Topics[1].TotalLag);
        Assert.Single(parsed.Topics[1].Partitions);
        Assert.Equal(50L, parsed.Topics[1].Partitions[0].LogStartOffset);
    }

    [Fact]
    public void GetGroupLagResponse_EmptyTopics_RoundTrips()
    {
        // Newly-created group with no committed offsets yet.
        var original = new GetGroupLagResponsePayload
        {
            ErrorCode = 0,
            GroupId = "g-new",
            State = "Empty",
            TotalLag = 0,
            PartitionCount = 0,
            MemberCount = 0,
            Topics = Array.Empty<TopicLagPayload>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetGroupLagResponsePayload.Read(ref r); });

        Assert.Equal("Empty", parsed.State);
        Assert.Empty(parsed.Topics);
    }

    [Fact]
    public void GetGroupLagResponse_ErrorPath_RoundTrips()
    {
        // GROUP_AUTHORIZATION_FAILED (30): topic/partition lists empty.
        var original = new GetGroupLagResponsePayload
        {
            ErrorCode = 30,
            GroupId = "g-forbidden",
            State = "",
            TotalLag = 0,
            PartitionCount = 0,
            MemberCount = 0,
            Topics = Array.Empty<TopicLagPayload>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetGroupLagResponsePayload.Read(ref r); });

        Assert.Equal((ushort)30, parsed.ErrorCode);
    }

    // ───────────────────────────────────────────────────────────────
    // GetLagSummary
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void GetLagSummaryResponse_RoundTrip_PreservesAggregatesAndMaxLagGroup()
    {
        var original = new GetLagSummaryResponsePayload
        {
            ErrorCode = 0,
            GroupCount = 12,
            GroupsWithHighLag = 2,
            TotalLag = 50_000_000L,
            MaxLag = 25_000_000L,
            MaxLagGroup = "g-batch-ingest",
            Groups = new[]
            {
                new LagSummaryGroupPayload { GroupId = "g-batch-ingest", State = "Stable", TotalLag = 25_000_000L, PartitionCount = 128, MemberCount = 8 },
                new LagSummaryGroupPayload { GroupId = "g-analytics",    State = "Stable", TotalLag = 15_000_000L, PartitionCount = 64,  MemberCount = 4 },
                new LagSummaryGroupPayload { GroupId = "g-realtime",     State = "Stable", TotalLag = 10_000_000L, PartitionCount = 16,  MemberCount = 2 },
            },
        };

        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetLagSummaryResponsePayload.Read(ref r); });

        Assert.Equal(12, parsed.GroupCount);
        Assert.Equal(2, parsed.GroupsWithHighLag);
        Assert.Equal(50_000_000L, parsed.TotalLag);
        Assert.Equal(25_000_000L, parsed.MaxLag);
        Assert.Equal("g-batch-ingest", parsed.MaxLagGroup);
        Assert.Equal(3, parsed.Groups.Length);

        Assert.Equal("g-batch-ingest", parsed.Groups[0].GroupId);
        Assert.Equal(128, parsed.Groups[0].PartitionCount);
        Assert.Equal(10_000_000L, parsed.Groups[2].TotalLag);
    }

    [Fact]
    public void GetLagSummaryResponse_NoGroups_RoundTrips()
    {
        var original = new GetLagSummaryResponsePayload
        {
            ErrorCode = 0,
            GroupCount = 0,
            GroupsWithHighLag = 0,
            TotalLag = 0,
            MaxLag = 0,
            MaxLagGroup = null,
            Groups = Array.Empty<LagSummaryGroupPayload>(),
        };
        // Pin the `Write` (not `WriteTo`) path — it preserves null
        // MaxLagGroup as null on read-back (WriteTo coerces to "").
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetLagSummaryResponsePayload.Read(ref r); });

        Assert.Equal(0, parsed.GroupCount);
        Assert.Null(parsed.MaxLagGroup);
        Assert.Empty(parsed.Groups);
    }
}

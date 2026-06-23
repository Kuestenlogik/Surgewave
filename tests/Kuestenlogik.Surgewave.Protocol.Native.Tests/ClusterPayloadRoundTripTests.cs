using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch for the Cluster sub-namespace of Protocol.Native.
/// Every payload in this namespace sat at 0% on the latest coverage
/// report; the round-trip pattern is mechanical
/// (<c>EstimateSize → Write → Read</c>) so the failure surface is
/// asymmetric framing bugs — a missing field or a wrong order shows up
/// as a corrupt round-trip.
///
/// Covers: <see cref="BrokerInfoPayload"/>, <see cref="ListBrokersPayload"/>,
/// <see cref="ClusterInfoPayload"/>, <see cref="CompactionResultPayload"/>,
/// <see cref="TopicCompactionStatusPayload"/>, <see cref="CompactionStatusPayload"/>.
/// </summary>
public sealed class ClusterPayloadRoundTripTests
{
    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    [Fact]
    public void BrokerInfoPayload_RoundTrip_PreservesAllFields()
    {
        var original = new BrokerInfoPayload
        {
            BrokerId = 17,
            Host = "broker-eu-west-1.kuestenlogik.de",
            Port = 9092,
            ReplicationPort = 19092,
            IsController = true,
            IsAlive = true,
            Rack = "rack-az-1a",
        };

        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return BrokerInfoPayload.Read(ref r); });

        Assert.Equal(17, parsed.BrokerId);
        Assert.Equal("broker-eu-west-1.kuestenlogik.de", parsed.Host);
        Assert.Equal(9092, parsed.Port);
        Assert.Equal(19092, parsed.ReplicationPort);
        Assert.True(parsed.IsController);
        Assert.True(parsed.IsAlive);
        Assert.Equal("rack-az-1a", parsed.Rack);
    }

    [Fact]
    public void BrokerInfoPayload_NullRack_RoundTrips()
    {
        // The Rack field is nullable on the wire (Read can return null);
        // the Write side passes the value through. Pin both ends.
        var original = new BrokerInfoPayload
        {
            BrokerId = 1,
            Host = "h",
            Port = 1,
            ReplicationPort = 2,
            IsController = false,
            IsAlive = false,
            Rack = null,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return BrokerInfoPayload.Read(ref r); });

        Assert.Null(parsed.Rack);
    }

    [Fact]
    public void ListBrokersPayload_RoundTrip_PreservesOrdering()
    {
        var original = new ListBrokersPayload
        {
            Brokers =
            [
                new BrokerInfoPayload { BrokerId = 1, Host = "b1", Port = 9092, ReplicationPort = 19092, IsController = true,  IsAlive = true,  Rack = "az-a" },
                new BrokerInfoPayload { BrokerId = 2, Host = "b2", Port = 9092, ReplicationPort = 19092, IsController = false, IsAlive = true,  Rack = "az-b" },
                new BrokerInfoPayload { BrokerId = 3, Host = "b3", Port = 9092, ReplicationPort = 19092, IsController = false, IsAlive = false, Rack = null  },
            ],
        };

        var sizeEstimate = 4 + original.Brokers.Sum(b => b.EstimateSize());
        var parsed = RoundTrip(
            sizeEstimate,
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ListBrokersPayload.Read(ref r); });

        Assert.Equal(3, parsed.Brokers.Count);
        Assert.Equal(1, parsed.Brokers[0].BrokerId);
        Assert.True(parsed.Brokers[0].IsController);
        Assert.Equal("b3", parsed.Brokers[2].Host);
        Assert.False(parsed.Brokers[2].IsAlive);
        Assert.Null(parsed.Brokers[2].Rack);
    }

    [Fact]
    public void ListBrokersPayload_EmptyList_RoundTrips()
    {
        var original = new ListBrokersPayload { Brokers = Array.Empty<BrokerInfoPayload>() };
        var parsed = RoundTrip(
            16,
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ListBrokersPayload.Read(ref r); });
        Assert.Empty(parsed.Brokers);
    }

    [Fact]
    public void ClusterInfoPayload_RoundTrip_PreservesAllFields()
    {
        var original = new ClusterInfoPayload
        {
            BrokerId = 42,
            Host = "ctrl-host",
            Port = 9092,
            IsController = true,
            ControllerId = 42,
            ControllerEpoch = 7,
            UseRaftConsensus = true,
            IsRaftLeader = true,
            RaftTerm = 4,
            TopicCount = 128,
            TotalPartitions = 1024,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ClusterInfoPayload.Read(ref r); });

        Assert.Equal(42, parsed.BrokerId);
        Assert.Equal("ctrl-host", parsed.Host);
        Assert.True(parsed.IsController);
        Assert.Equal(7, parsed.ControllerEpoch);
        Assert.True(parsed.UseRaftConsensus);
        Assert.True(parsed.IsRaftLeader);
        Assert.Equal(4, parsed.RaftTerm);
        Assert.Equal(128, parsed.TopicCount);
        Assert.Equal(1024, parsed.TotalPartitions);
    }

    [Fact]
    public void ClusterInfoPayload_NonRaftFollower_RoundTrips()
    {
        // Non-Raft, non-controller broker — all the boolean toggles flip
        // independently; pin the off-state shape too.
        var original = new ClusterInfoPayload
        {
            BrokerId = 2,
            Host = "follower",
            Port = 9092,
            IsController = false,
            ControllerId = 1,
            ControllerEpoch = 3,
            UseRaftConsensus = false,
            IsRaftLeader = false,
            RaftTerm = 0,
            TopicCount = 0,
            TotalPartitions = 0,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ClusterInfoPayload.Read(ref r); });

        Assert.False(parsed.IsController);
        Assert.False(parsed.UseRaftConsensus);
        Assert.False(parsed.IsRaftLeader);
        Assert.Equal(0, parsed.RaftTerm);
    }

    [Fact]
    public void CompactionResultPayload_RoundTrip_PreservesAllFields()
    {
        var original = new CompactionResultPayload
        {
            Success = true,
            RecordsRemoved = 1_234_567L,
            BytesRemoved = 9_876_543_210L,
            SegmentsCompacted = 42,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CompactionResultPayload.Read(ref r); });

        Assert.True(parsed.Success);
        Assert.Equal(1_234_567L, parsed.RecordsRemoved);
        Assert.Equal(9_876_543_210L, parsed.BytesRemoved);
        Assert.Equal(42, parsed.SegmentsCompacted);
    }

    [Fact]
    public void CompactionResultPayload_FailurePath_RoundTrips()
    {
        var original = new CompactionResultPayload { Success = false, RecordsRemoved = 0, BytesRemoved = 0, SegmentsCompacted = 0 };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CompactionResultPayload.Read(ref r); });

        Assert.False(parsed.Success);
    }

    [Fact]
    public void TopicCompactionStatusPayload_RoundTrip_PreservesAllFields()
    {
        var original = new TopicCompactionStatusPayload
        {
            Topic = "high-volume-events",
            PartitionCount = 64,
            CleanupPolicy = "compact,delete",
            SegmentCount = 1024,
            TotalBytes = 100_000_000_000L,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return TopicCompactionStatusPayload.Read(ref r); });

        Assert.Equal("high-volume-events", parsed.Topic);
        Assert.Equal(64, parsed.PartitionCount);
        Assert.Equal("compact,delete", parsed.CleanupPolicy);
        Assert.Equal(1024, parsed.SegmentCount);
        Assert.Equal(100_000_000_000L, parsed.TotalBytes);
    }

    [Fact]
    public void CompactionStatusPayload_RoundTrip_AggregatesMultipleTopics()
    {
        var original = new CompactionStatusPayload
        {
            Topics =
            [
                new TopicCompactionStatusPayload { Topic = "t1", PartitionCount = 4, CleanupPolicy = "compact", SegmentCount = 12, TotalBytes = 1_000_000 },
                new TopicCompactionStatusPayload { Topic = "t2", PartitionCount = 8, CleanupPolicy = "delete",  SegmentCount = 48, TotalBytes = 8_000_000 },
            ],
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CompactionStatusPayload.Read(ref r); });

        Assert.Equal(2, parsed.Topics.Count);
        Assert.Equal("t1", parsed.Topics[0].Topic);
        Assert.Equal("compact", parsed.Topics[0].CleanupPolicy);
        Assert.Equal(8_000_000L, parsed.Topics[1].TotalBytes);
    }

    [Fact]
    public void CompactionStatusPayload_EmptyTopicList_RoundTrips()
    {
        var original = new CompactionStatusPayload { Topics = Array.Empty<TopicCompactionStatusPayload>() };
        var parsed = RoundTrip(
            16,
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CompactionStatusPayload.Read(ref r); });
        Assert.Empty(parsed.Topics);
    }
}

using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ShareGroups;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — ShareGroup offsets admin payloads. Covers the
/// Describe/Alter/Delete trio on the native wire (each Request +
/// Response, so six payloads + their seven nested record-structs).
///
/// These back the <c>surgewave share-group offsets ...</c> CLI and the
/// Control-UI's share-consumer offsets tab. Framing regressions here
/// surface as "Describe returned no partitions" or "Alter looked
/// successful but offsets didn't move" — both annoying to root-cause
/// without a wire-shape pin.
/// </summary>
public sealed class ShareGroupOffsetsRoundTripTests
{
    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // DescribeShareGroupOffsets
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeRequest_RoundTrip_PreservesGroupAndTopicList()
    {
        var original = new DescribeShareGroupOffsetsRequestPayload
        {
            GroupId = "share-orders",
            Topics = new[]
            {
                new ShareGroupOffsetsTopic("orders", new[] { 0, 1, 2 }),
                new ShareGroupOffsetsTopic("audit",  new[] { 0 }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeShareGroupOffsetsRequestPayload.Read(ref r); });

        Assert.Equal("share-orders", parsed.GroupId);
        Assert.Equal(2, parsed.Topics.Length);
        Assert.Equal("orders", parsed.Topics[0].TopicName);
        Assert.Equal(new[] { 0, 1, 2 }, parsed.Topics[0].Partitions);
        Assert.Equal("audit", parsed.Topics[1].TopicName);
    }

    [Fact]
    public void DescribeRequest_EmptyTopicList_RoundTrips()
    {
        var original = new DescribeShareGroupOffsetsRequestPayload
        {
            GroupId = "share-empty",
            Topics = Array.Empty<ShareGroupOffsetsTopic>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeShareGroupOffsetsRequestPayload.Read(ref r); });
        Assert.Empty(parsed.Topics);
    }

    [Fact]
    public void DescribeResponse_FullShape_RoundTrips()
    {
        var original = new DescribeShareGroupOffsetsResponsePayload
        {
            Groups = new[]
            {
                new DescribeShareGroupOffsetsGroup("share-orders", new[]
                {
                    new DescribeShareGroupOffsetsTopicResponse("orders", new[]
                    {
                        new ShareGroupOffsetPartitionResponse
                        {
                            PartitionIndex = 0,
                            StartOffset = 100_000L,
                            LeaderEpoch = 4,
                            ErrorCode = 0,
                        },
                        new ShareGroupOffsetPartitionResponse
                        {
                            PartitionIndex = 1,
                            StartOffset = 99_999L,
                            LeaderEpoch = 4,
                            ErrorCode = 0,
                        },
                    }),
                    new DescribeShareGroupOffsetsTopicResponse("audit", new[]
                    {
                        new ShareGroupOffsetPartitionResponse
                        {
                            PartitionIndex = 0,
                            StartOffset = 50L,
                            LeaderEpoch = 1,
                            ErrorCode = 0,
                        },
                    }),
                }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeShareGroupOffsetsResponsePayload.Read(ref r); });

        Assert.Single(parsed.Groups);
        Assert.Equal("share-orders", parsed.Groups[0].GroupId);
        Assert.Equal(2, parsed.Groups[0].Topics.Length);

        var ordersTopic = parsed.Groups[0].Topics[0];
        Assert.Equal("orders", ordersTopic.TopicName);
        Assert.Equal(2, ordersTopic.Partitions.Length);
        Assert.Equal(100_000L, ordersTopic.Partitions[0].StartOffset);
        Assert.Equal((short)4, (short)ordersTopic.Partitions[0].LeaderEpoch);
        Assert.Equal(99_999L, ordersTopic.Partitions[1].StartOffset);

        Assert.Equal("audit", parsed.Groups[0].Topics[1].TopicName);
        Assert.Equal(50L, parsed.Groups[0].Topics[1].Partitions[0].StartOffset);
    }

    [Fact]
    public void DescribeResponse_PerPartitionError_RoundTrips()
    {
        // UNKNOWN_TOPIC_OR_PARTITION (3) on one partition while others
        // succeed — pin the per-partition error path.
        var original = new DescribeShareGroupOffsetsResponsePayload
        {
            Groups = new[]
            {
                new DescribeShareGroupOffsetsGroup("share-orders", new[]
                {
                    new DescribeShareGroupOffsetsTopicResponse("orders", new[]
                    {
                        new ShareGroupOffsetPartitionResponse { PartitionIndex = 0, StartOffset = 100, LeaderEpoch = 1, ErrorCode = 0 },
                        new ShareGroupOffsetPartitionResponse { PartitionIndex = 99, StartOffset = -1, LeaderEpoch = -1, ErrorCode = 3 },
                    }),
                }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeShareGroupOffsetsResponsePayload.Read(ref r); });

        var partitions = parsed.Groups[0].Topics[0].Partitions;
        Assert.Equal((short)0, partitions[0].ErrorCode);
        Assert.Equal((short)3, partitions[1].ErrorCode);
        Assert.Equal(-1, partitions[1].LeaderEpoch);
    }

    // ───────────────────────────────────────────────────────────────
    // AlterShareGroupOffsets
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void AlterRequest_RoundTrip_PreservesPartitionOffsets()
    {
        var original = new AlterShareGroupOffsetsRequestPayload
        {
            GroupId = "share-orders",
            Topics = new[]
            {
                new AlterShareGroupOffsetsTopic("orders", new[]
                {
                    new AlterShareGroupOffsetsPartition(0, 12_345L),
                    new AlterShareGroupOffsetsPartition(1, 6_789L),
                }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return AlterShareGroupOffsetsRequestPayload.Read(ref r); });

        Assert.Equal("share-orders", parsed.GroupId);
        Assert.Single(parsed.Topics);
        Assert.Equal("orders", parsed.Topics[0].TopicName);
        Assert.Equal(12_345L, parsed.Topics[0].Partitions[0].StartOffset);
        Assert.Equal(6_789L, parsed.Topics[0].Partitions[1].StartOffset);
    }

    [Fact]
    public void AlterResponse_RoundTrip_ReusesDescribeResponseShape()
    {
        // AlterResponse intentionally shares the DescribeResponse wire
        // shape — pin a non-trivial round-trip so a future "let's
        // optimise this" doesn't drift them apart silently.
        var original = new AlterShareGroupOffsetsResponsePayload
        {
            Groups = new[]
            {
                new DescribeShareGroupOffsetsGroup("share-orders", new[]
                {
                    new DescribeShareGroupOffsetsTopicResponse("orders", new[]
                    {
                        new ShareGroupOffsetPartitionResponse
                        {
                            PartitionIndex = 0,
                            StartOffset = 12_345L,
                            LeaderEpoch = 5,
                            ErrorCode = 0,
                        },
                    }),
                }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return AlterShareGroupOffsetsResponsePayload.Read(ref r); });

        Assert.Single(parsed.Groups);
        Assert.Equal(12_345L, parsed.Groups[0].Topics[0].Partitions[0].StartOffset);
        Assert.Equal((short)5, (short)parsed.Groups[0].Topics[0].Partitions[0].LeaderEpoch);
    }

    // ───────────────────────────────────────────────────────────────
    // DeleteShareGroupOffsets
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DeleteRequest_RoundTrip_PreservesGroupAndTopicNames()
    {
        var original = new DeleteShareGroupOffsetsRequestPayload
        {
            GroupId = "share-orders",
            Topics = new[] { "orders", "events", "audit" },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DeleteShareGroupOffsetsRequestPayload.Read(ref r); });

        Assert.Equal("share-orders", parsed.GroupId);
        Assert.Equal(new[] { "orders", "events", "audit" }, parsed.Topics);
    }

    [Fact]
    public void DeleteResponse_FullShape_WithMixedErrors_RoundTrips()
    {
        // Topic-level error (no partitions emitted) + per-partition
        // results in the same response.
        var original = new DeleteShareGroupOffsetsResponsePayload
        {
            Results = new[]
            {
                new DeleteShareGroupOffsetsResult("orders", ErrorCode: 0, Partitions: new[]
                {
                    new DeleteShareGroupOffsetsPartitionResult(0, 0),
                    new DeleteShareGroupOffsetsPartitionResult(1, 0),
                }),
                new DeleteShareGroupOffsetsResult("forbidden", ErrorCode: 30 /* GROUP_AUTHORIZATION_FAILED */, Partitions: Array.Empty<DeleteShareGroupOffsetsPartitionResult>()),
                new DeleteShareGroupOffsetsResult("audit", ErrorCode: 0, Partitions: new[]
                {
                    new DeleteShareGroupOffsetsPartitionResult(0, 0),
                    new DeleteShareGroupOffsetsPartitionResult(99, 3 /* UNKNOWN_TOPIC_OR_PARTITION */),
                }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DeleteShareGroupOffsetsResponsePayload.Read(ref r); });

        Assert.Equal(3, parsed.Results.Length);
        Assert.Equal((short)0, parsed.Results[0].ErrorCode);
        Assert.Equal(2, parsed.Results[0].Partitions.Length);
        Assert.Equal((short)30, parsed.Results[1].ErrorCode);
        Assert.Empty(parsed.Results[1].Partitions);
        Assert.Equal((short)3, parsed.Results[2].Partitions[1].ErrorCode);
        Assert.Equal(99, parsed.Results[2].Partitions[1].PartitionIndex);
    }
}

using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ShareGroups;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — KIP-932 ShareGroup heartbeat + describe payloads
/// on the native wire. Covers the four client-facing payloads in
/// Protocol.Native.Payloads.ShareGroups that drive the share-consumer
/// rebalance flow: <see cref="ShareGroupHeartbeatRequestPayload"/> +
/// Response, <see cref="ShareGroupDescribeRequestPayload"/> + the public
/// shape of the Describe response.
///
/// ShareGroup offsets payloads (Alter/Delete/Describe) plus ShareFetch /
/// ShareAcknowledge are a separate follow-up batch — they share the
/// same Read/Write/EstimateSize pattern but have larger nested shapes
/// and deserve focused pins of their own.
/// </summary>
public sealed class ShareGroupHeartbeatDescribeRoundTripTests
{
    private static readonly Guid TopicA = new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid TopicB = new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // ShareGroupHeartbeat
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void HeartbeatRequest_FullShape_RoundTrips()
    {
        var original = new ShareGroupHeartbeatRequestPayload
        {
            GroupId = "share-orders",
            MemberId = "consumer-1",
            MemberEpoch = 5,
            RackId = "az-eu-west-1a",
            SubscribedTopicNames = new[] { "orders", "audit" },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareGroupHeartbeatRequestPayload.Read(ref r); });

        Assert.Equal("share-orders", parsed.GroupId);
        Assert.Equal("consumer-1", parsed.MemberId);
        Assert.Equal(5, parsed.MemberEpoch);
        Assert.Equal("az-eu-west-1a", parsed.RackId);
        Assert.Equal(new[] { "orders", "audit" }, parsed.SubscribedTopicNames);
    }

    [Fact]
    public void HeartbeatRequest_NullSubscribedTopics_WireDistinctFromEmpty()
    {
        // The wire uses int32=-1 for null vs int32=0 for empty. Pin both
        // ends — a confusion of the two changes rebalance semantics from
        // "preserve subscription" to "unsubscribe from everything".
        var nullCase = new ShareGroupHeartbeatRequestPayload
        {
            GroupId = "g",
            MemberId = "m",
            MemberEpoch = 1,
            RackId = null,
            SubscribedTopicNames = null,
        };
        var emptyCase = nullCase with { SubscribedTopicNames = Array.Empty<string>() };

        var nullParsed = RoundTrip(
            nullCase.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); nullCase.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareGroupHeartbeatRequestPayload.Read(ref r); });
        var emptyParsed = RoundTrip(
            emptyCase.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); emptyCase.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareGroupHeartbeatRequestPayload.Read(ref r); });

        Assert.Null(nullParsed.SubscribedTopicNames);
        Assert.NotNull(emptyParsed.SubscribedTopicNames);
        Assert.Empty(emptyParsed.SubscribedTopicNames!);
    }

    [Fact]
    public void HeartbeatResponse_FullShape_RoundTrips()
    {
        var original = new ShareGroupHeartbeatResponsePayload
        {
            ThrottleTimeMs = 0,
            ErrorCode = 0,
            MemberId = "consumer-1",
            MemberEpoch = 6,
            HeartbeatIntervalMs = 5_000,
            Assignment = new[]
            {
                new HeartbeatTopicPartition(TopicA, new[] { 0, 1, 2 }),
                new HeartbeatTopicPartition(TopicB, new[] { 4 }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareGroupHeartbeatResponsePayload.Read(ref r); });

        Assert.Equal("consumer-1", parsed.MemberId);
        Assert.Equal(6, parsed.MemberEpoch);
        Assert.Equal(5_000, parsed.HeartbeatIntervalMs);
        Assert.Equal(2, parsed.Assignment.Length);
        Assert.Equal(TopicA, parsed.Assignment[0].TopicId);
        Assert.Equal(new[] { 0, 1, 2 }, parsed.Assignment[0].Partitions);
        Assert.Equal(TopicB, parsed.Assignment[1].TopicId);
        Assert.Equal(new[] { 4 }, parsed.Assignment[1].Partitions);
    }

    [Fact]
    public void HeartbeatResponse_EmptyAssignment_RoundTrips()
    {
        // Member joined but assignor hasn't computed anything yet —
        // empty Assignment list is the legitimate steady-state for a
        // member with no work.
        var original = new ShareGroupHeartbeatResponsePayload
        {
            ThrottleTimeMs = 0,
            ErrorCode = 0,
            MemberId = "consumer-1",
            MemberEpoch = 1,
            HeartbeatIntervalMs = 5_000,
            Assignment = Array.Empty<HeartbeatTopicPartition>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareGroupHeartbeatResponsePayload.Read(ref r); });

        Assert.Empty(parsed.Assignment);
    }

    [Theory]
    [InlineData((short)0)]    // OK
    [InlineData((short)110)]  // FENCED_MEMBER_EPOCH
    [InlineData((short)112)]  // STALE_MEMBER_EPOCH
    public void HeartbeatResponse_AllErrorCodes_RoundTrip(short errorCode)
    {
        var original = new ShareGroupHeartbeatResponsePayload
        {
            ThrottleTimeMs = 0,
            ErrorCode = errorCode,
            MemberId = "consumer-1",
            MemberEpoch = errorCode == 0 ? 6 : -1,
            HeartbeatIntervalMs = 5_000,
            Assignment = Array.Empty<HeartbeatTopicPartition>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareGroupHeartbeatResponsePayload.Read(ref r); });
        Assert.Equal(errorCode, parsed.ErrorCode);
    }

    // ───────────────────────────────────────────────────────────────
    // ShareGroupDescribe
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeRequest_RoundTrip_PreservesGroupIds()
    {
        var original = new ShareGroupDescribeRequestPayload
        {
            GroupIds = new[] { "share-orders", "share-events", "share-audit" },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareGroupDescribeRequestPayload.Read(ref r); });

        Assert.Equal(new[] { "share-orders", "share-events", "share-audit" }, parsed.GroupIds);
    }

    [Fact]
    public void DescribeRequest_EmptyGroupIds_RoundTrips()
    {
        var original = new ShareGroupDescribeRequestPayload { GroupIds = Array.Empty<string>() };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareGroupDescribeRequestPayload.Read(ref r); });
        Assert.Empty(parsed.GroupIds);
    }
}

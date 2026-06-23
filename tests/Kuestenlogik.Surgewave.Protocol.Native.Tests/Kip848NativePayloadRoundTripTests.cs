using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — the KIP-848 native payloads
/// (<see cref="ConsumerGroupHeartbeatRequestPayload"/> + Response,
/// <see cref="ConsumerGroupDescribeRequestPayload"/> + Response).
/// These are the most-shaped payloads in the ConsumerGroups namespace
/// — nullable strings, nullable arrays, GUID topic-ids, and (on
/// Describe) per-member nested assignment lists — so they deserve their
/// own focused round-trip pin rather than getting batched with the
/// classic admin RPCs.
///
/// All four were sitting at 0% on the latest report; KIP-848 is a Done
/// KIP in Surgewave (see kips.md), so the absence of native-wire tests
/// was a real gap in the regression net for the consumer-group v2
/// surface.
/// </summary>
public sealed class Kip848NativePayloadRoundTripTests
{
    private static readonly Guid TopicA = new("11111111-1111-1111-1111-111111111111");
    private static readonly Guid TopicB = new("22222222-2222-2222-2222-222222222222");

    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // ConsumerGroupHeartbeat (the dominant KIP-848 RPC — every consumer
    // sends one per heartbeat-interval)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void HeartbeatRequest_FullShape_RoundTrips()
    {
        var original = new ConsumerGroupHeartbeatRequestPayload
        {
            GroupId = "g-prod",
            MemberId = "consumer-abc-123",
            MemberEpoch = 5,
            InstanceId = "static-id-1",
            RackId = "az-1a",
            RebalanceTimeoutMs = 60_000,
            SubscribedTopicNames = new[] { "events", "audit" },
            ServerAssignor = "uniform",
            TopicPartitions = new[]
            {
                new TopicPartitionAssignment(TopicA, new[] { 0, 1, 2 }),
                new TopicPartitionAssignment(TopicB, new[] { 4 }),
            },
        };

        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConsumerGroupHeartbeatRequestPayload.Read(ref r); });

        Assert.Equal("g-prod", parsed.GroupId);
        Assert.Equal("consumer-abc-123", parsed.MemberId);
        Assert.Equal(5, parsed.MemberEpoch);
        Assert.Equal("static-id-1", parsed.InstanceId);
        Assert.Equal("az-1a", parsed.RackId);
        Assert.Equal(60_000, parsed.RebalanceTimeoutMs);
        Assert.Equal(new[] { "events", "audit" }, parsed.SubscribedTopicNames);
        Assert.Equal("uniform", parsed.ServerAssignor);
        Assert.Equal(2, parsed.TopicPartitions.Length);
        Assert.Equal(TopicA, parsed.TopicPartitions[0].TopicId);
        Assert.Equal(new[] { 0, 1, 2 }, parsed.TopicPartitions[0].Partitions);
        Assert.Equal(TopicB, parsed.TopicPartitions[1].TopicId);
        Assert.Equal(new[] { 4 }, parsed.TopicPartitions[1].Partitions);
    }

    [Fact]
    public void HeartbeatRequest_JoinShape_NullsAndEmpties_RoundTrip()
    {
        // First heartbeat — MemberId empty, no InstanceId, no rack, no
        // owned partitions yet. Pins that all the nullable-marker bytes
        // round-trip cleanly when they're effectively "absent".
        var original = new ConsumerGroupHeartbeatRequestPayload
        {
            GroupId = "g-new",
            MemberId = "",
            MemberEpoch = 0,
            InstanceId = null,
            RackId = null,
            RebalanceTimeoutMs = 30_000,
            SubscribedTopicNames = new[] { "events" },
            ServerAssignor = null,
            TopicPartitions = Array.Empty<TopicPartitionAssignment>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConsumerGroupHeartbeatRequestPayload.Read(ref r); });

        Assert.Equal("", parsed.MemberId);
        Assert.Equal(0, parsed.MemberEpoch);
        Assert.Null(parsed.InstanceId);
        Assert.Null(parsed.RackId);
        Assert.Null(parsed.ServerAssignor);
        Assert.Empty(parsed.TopicPartitions);
    }

    [Fact]
    public void HeartbeatRequest_NullSubscribedTopics_RoundTrips()
    {
        // The SubscribedTopicNames field uses a bool prefix — null and
        // empty-array are wire-distinct. Pin both endpoints.
        var original = new ConsumerGroupHeartbeatRequestPayload
        {
            GroupId = "g",
            MemberId = "m",
            MemberEpoch = 1,
            InstanceId = null,
            RackId = null,
            RebalanceTimeoutMs = 30_000,
            SubscribedTopicNames = null,
            ServerAssignor = null,
            TopicPartitions = Array.Empty<TopicPartitionAssignment>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConsumerGroupHeartbeatRequestPayload.Read(ref r); });
        Assert.Null(parsed.SubscribedTopicNames);
    }

    [Fact]
    public void HeartbeatResponse_FullShape_RoundTrips()
    {
        var original = new ConsumerGroupHeartbeatResponsePayload
        {
            ThrottleTimeMs = 0,
            ErrorCode = 0,
            ErrorMessage = null,
            MemberId = "consumer-abc-123",
            MemberEpoch = 6,
            HeartbeatIntervalMs = 5_000,
            Assignment = new[]
            {
                new TopicPartitionAssignment(TopicA, new[] { 0, 1 }),
                new TopicPartitionAssignment(TopicB, new[] { 2, 3 }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConsumerGroupHeartbeatResponsePayload.Read(ref r); });

        Assert.Equal(0, parsed.ThrottleTimeMs);
        Assert.Equal(0, parsed.ErrorCode);
        Assert.Null(parsed.ErrorMessage);
        Assert.Equal("consumer-abc-123", parsed.MemberId);
        Assert.Equal(6, parsed.MemberEpoch);
        Assert.Equal(5_000, parsed.HeartbeatIntervalMs);
        Assert.NotNull(parsed.Assignment);
        Assert.Equal(2, parsed.Assignment.Length);
        Assert.Equal(TopicA, parsed.Assignment[0].TopicId);
        Assert.Equal(new[] { 0, 1 }, parsed.Assignment[0].Partitions);
    }

    [Fact]
    public void HeartbeatResponse_ErrorPath_RoundTrips()
    {
        // FENCED_MEMBER_EPOCH (110 in upstream): broker tells the client
        // the member is stale. ErrorMessage carries the human-readable
        // reason, MemberId is preserved so the client can clear it,
        // Assignment is null (no new assignment to communicate).
        var original = new ConsumerGroupHeartbeatResponsePayload
        {
            ThrottleTimeMs = 0,
            ErrorCode = 110,
            ErrorMessage = "Member epoch 5 is fenced — current epoch is 7",
            MemberId = "consumer-abc-123",
            MemberEpoch = -1,
            HeartbeatIntervalMs = 5_000,
            Assignment = null,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConsumerGroupHeartbeatResponsePayload.Read(ref r); });

        Assert.Equal((short)110, parsed.ErrorCode);
        Assert.Equal("Member epoch 5 is fenced — current epoch is 7", parsed.ErrorMessage);
        Assert.Equal(-1, parsed.MemberEpoch);
        Assert.Null(parsed.Assignment);
    }

    // ───────────────────────────────────────────────────────────────
    // ConsumerGroupDescribe (admin / Control-UI: rich member detail)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeRequest_RoundTrip_PreservesGroupIdsAndFlag()
    {
        var original = new ConsumerGroupDescribeRequestPayload
        {
            GroupIds = new[] { "g-1", "g-2", "g-3" },
            IncludeAuthorizedOperations = true,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConsumerGroupDescribeRequestPayload.Read(ref r); });

        Assert.Equal(new[] { "g-1", "g-2", "g-3" }, parsed.GroupIds);
        Assert.True(parsed.IncludeAuthorizedOperations);
    }

    [Fact]
    public void DescribeRequest_EmptyGroupIds_RoundTrips()
    {
        var original = new ConsumerGroupDescribeRequestPayload
        {
            GroupIds = Array.Empty<string>(),
            IncludeAuthorizedOperations = false,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConsumerGroupDescribeRequestPayload.Read(ref r); });

        Assert.Empty(parsed.GroupIds);
        Assert.False(parsed.IncludeAuthorizedOperations);
    }

    [Fact]
    public void DescribeResponse_FullShape_RoundTrips()
    {
        var original = new ConsumerGroupDescribeResponsePayload
        {
            ThrottleTimeMs = 0,
            Groups = new[]
            {
                new DescribedConsumerGroup
                {
                    ErrorCode = 0,
                    GroupId = "g-prod",
                    GroupState = "Stable",
                    GroupEpoch = 12,
                    AssignmentEpoch = 12,
                    AssignorName = "uniform",
                    Members = new[]
                    {
                        new ConsumerGroupMember
                        {
                            MemberId = "m1",
                            InstanceId = "static-1",
                            RackId = "az-a",
                            ClientId = "client-1",
                            ClientHost = "/10.0.0.1",
                            SubscribedTopicNames = new[] { "events", "audit" },
                            SubscribedTopicRegex = null,
                            Assignment = new[]
                            {
                                new TopicPartitionAssignment(TopicA, new[] { 0, 1 }),
                            },
                            TargetAssignment = new[]
                            {
                                new TopicPartitionAssignment(TopicA, new[] { 0, 1, 2 }),
                            },
                        },
                        new ConsumerGroupMember
                        {
                            MemberId = "m2",
                            InstanceId = null, // dynamic member
                            RackId = null,
                            ClientId = "client-2",
                            ClientHost = "/10.0.0.2",
                            SubscribedTopicNames = new[] { "events" },
                            SubscribedTopicRegex = "^events\\..*$",
                            Assignment = Array.Empty<TopicPartitionAssignment>(),
                            TargetAssignment = Array.Empty<TopicPartitionAssignment>(),
                        },
                    },
                },
            },
        };

        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConsumerGroupDescribeResponsePayload.Read(ref r); });

        Assert.Single(parsed.Groups);
        var group = parsed.Groups[0];
        Assert.Equal("g-prod", group.GroupId);
        Assert.Equal("Stable", group.GroupState);
        Assert.Equal(12, group.GroupEpoch);
        Assert.Equal(12, group.AssignmentEpoch);
        Assert.Equal("uniform", group.AssignorName);
        Assert.Equal(2, group.Members.Length);

        // Member 1: static, with rack, full assignment shape (incl. target ≠ current)
        var m1 = group.Members[0];
        Assert.Equal("m1", m1.MemberId);
        Assert.Equal("static-1", m1.InstanceId);
        Assert.Equal("az-a", m1.RackId);
        Assert.Equal("/10.0.0.1", m1.ClientHost);
        Assert.Equal(new[] { "events", "audit" }, m1.SubscribedTopicNames);
        Assert.Null(m1.SubscribedTopicRegex);
        Assert.Equal(new[] { 0, 1 }, m1.Assignment[0].Partitions);
        Assert.Equal(new[] { 0, 1, 2 }, m1.TargetAssignment[0].Partitions);

        // Member 2: dynamic, no rack, regex subscription, no assignment yet
        var m2 = group.Members[1];
        Assert.Null(m2.InstanceId);
        Assert.Null(m2.RackId);
        Assert.Equal("^events\\..*$", m2.SubscribedTopicRegex);
        Assert.Empty(m2.Assignment);
        Assert.Empty(m2.TargetAssignment);
    }

    [Fact]
    public void DescribeResponse_EmptyGroups_RoundTrips()
    {
        var original = new ConsumerGroupDescribeResponsePayload
        {
            ThrottleTimeMs = 0,
            Groups = Array.Empty<DescribedConsumerGroup>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConsumerGroupDescribeResponsePayload.Read(ref r); });
        Assert.Empty(parsed.Groups);
    }

    [Fact]
    public void DescribeResponse_GroupError_RoundTrips()
    {
        // Per-group error: GROUP_AUTHORIZATION_FAILED (30). The Members
        // array is empty in this branch — pin that.
        var original = new ConsumerGroupDescribeResponsePayload
        {
            ThrottleTimeMs = 0,
            Groups = new[]
            {
                new DescribedConsumerGroup
                {
                    ErrorCode = 30,
                    GroupId = "g-forbidden",
                    GroupState = "",
                    GroupEpoch = 0,
                    AssignmentEpoch = 0,
                    AssignorName = "",
                    Members = Array.Empty<ConsumerGroupMember>(),
                },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConsumerGroupDescribeResponsePayload.Read(ref r); });
        Assert.Equal((short)30, parsed.Groups[0].ErrorCode);
        Assert.Empty(parsed.Groups[0].Members);
    }
}

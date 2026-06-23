using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.StreamsGroups;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — KIP-1071 Streams Rebalance Protocol payloads on
/// the native wire. Covers all four 0%-coverage payloads
/// (StreamsGroupHeartbeat + StreamsGroupDescribe, each Request + Response)
/// plus the nested <see cref="SubtopologyInfo"/>,
/// <see cref="StreamsTaskId"/>, <see cref="StreamsTaskAssignment"/>,
/// <see cref="DescribedStreamsGroup"/>, and <see cref="StreamsGroupMember"/>
/// shapes that travel inside them.
///
/// KIP-1071 is the broker-driven streams-rebalance protocol that
/// Surgewave's Akka.Streams.Surgewave adapter targets. Framing pins on
/// this surface catch regressions before they reach the adapter.
/// </summary>
public sealed class Kip1071StreamsGroupPayloadRoundTripTests
{
    private static readonly Guid ProcessIdA = new("aaaaaaaa-1111-2222-3333-444444444444");
    private static readonly Guid ProcessIdB = new("bbbbbbbb-1111-2222-3333-555555555555");

    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // StreamsGroupHeartbeat
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void HeartbeatRequest_FullShape_RoundTrips()
    {
        var original = new StreamsGroupHeartbeatRequestPayload
        {
            GroupId = "wordcount-app",
            MemberId = "instance-1",
            MemberEpoch = 7,
            InstanceId = "static-1",
            RackId = "az-a",
            ProcessId = ProcessIdA,
            TopologyEpoch = 3,
            Subtopologies = new[]
            {
                new SubtopologyInfo(
                    "0", // subtopology id
                    new[] { "input-events", "input-audit" }, // sources
                    new[] { "wordcount-repartition" }),       // repartition sinks
                new SubtopologyInfo("1", Array.Empty<string>(), Array.Empty<string>()),
            },
            ActiveTasks = new[]
            {
                new StreamsTaskId("0", 0),
                new StreamsTaskId("0", 1),
            },
            StandbyTasks = new[] { new StreamsTaskId("1", 2) },
            WarmupTasks = Array.Empty<StreamsTaskId>(),
            ShutdownApplication = false,
        };

        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return StreamsGroupHeartbeatRequestPayload.Read(ref r); });

        Assert.Equal("wordcount-app", parsed.GroupId);
        Assert.Equal(ProcessIdA, parsed.ProcessId);
        Assert.Equal(3, parsed.TopologyEpoch);
        Assert.Equal(2, parsed.Subtopologies.Length);

        Assert.Equal("0", parsed.Subtopologies[0].SubtopologyId);
        Assert.Equal(new[] { "input-events", "input-audit" }, parsed.Subtopologies[0].SourceTopics);
        Assert.Equal(new[] { "wordcount-repartition" }, parsed.Subtopologies[0].RepartitionSinkTopics);
        Assert.Empty(parsed.Subtopologies[1].SourceTopics);

        Assert.Equal(2, parsed.ActiveTasks.Length);
        Assert.Equal("0", parsed.ActiveTasks[0].SubtopologyId);
        Assert.Equal(0, parsed.ActiveTasks[0].PartitionId);
        Assert.Single(parsed.StandbyTasks);
        Assert.Empty(parsed.WarmupTasks);
        Assert.False(parsed.ShutdownApplication);
    }

    [Fact]
    public void HeartbeatRequest_JoinShape_NullsAndEmpties_RoundTrip()
    {
        // First heartbeat: member doesn't know its memberId/topology yet.
        var original = new StreamsGroupHeartbeatRequestPayload
        {
            GroupId = "new-app",
            MemberId = "",
            MemberEpoch = 0,
            InstanceId = null,
            RackId = null,
            ProcessId = ProcessIdB,
            TopologyEpoch = 0,
            Subtopologies = Array.Empty<SubtopologyInfo>(),
            ActiveTasks = Array.Empty<StreamsTaskId>(),
            StandbyTasks = Array.Empty<StreamsTaskId>(),
            WarmupTasks = Array.Empty<StreamsTaskId>(),
            ShutdownApplication = false,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return StreamsGroupHeartbeatRequestPayload.Read(ref r); });

        Assert.Equal("", parsed.MemberId);
        Assert.Null(parsed.InstanceId);
        Assert.Null(parsed.RackId);
        Assert.Empty(parsed.Subtopologies);
        Assert.Empty(parsed.ActiveTasks);
    }

    [Fact]
    public void HeartbeatRequest_ShutdownSignal_RoundTrips()
    {
        // Application is shutting down — broker should retire the member's
        // tasks. Pin the wire shape of the shutdown signal.
        var original = new StreamsGroupHeartbeatRequestPayload
        {
            GroupId = "wordcount-app",
            MemberId = "instance-1",
            MemberEpoch = 7,
            InstanceId = null,
            RackId = null,
            ProcessId = ProcessIdA,
            TopologyEpoch = 3,
            Subtopologies = Array.Empty<SubtopologyInfo>(),
            ActiveTasks = Array.Empty<StreamsTaskId>(),
            StandbyTasks = Array.Empty<StreamsTaskId>(),
            WarmupTasks = Array.Empty<StreamsTaskId>(),
            ShutdownApplication = true,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return StreamsGroupHeartbeatRequestPayload.Read(ref r); });

        Assert.True(parsed.ShutdownApplication);
    }

    [Fact]
    public void HeartbeatResponse_FullShape_RoundTrips()
    {
        var original = new StreamsGroupHeartbeatResponsePayload
        {
            ThrottleTimeMs = 0,
            ErrorCode = 0,
            ErrorMessage = null,
            MemberId = "instance-1",
            MemberEpoch = 8,
            HeartbeatIntervalMs = 5_000,
            Assignment = new StreamsTaskAssignment(
                ActiveTasks: new[] { new StreamsTaskId("0", 0), new StreamsTaskId("0", 1) },
                StandbyTasks: new[] { new StreamsTaskId("1", 0) },
                WarmupTasks: Array.Empty<StreamsTaskId>()),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return StreamsGroupHeartbeatResponsePayload.Read(ref r); });

        Assert.Equal("instance-1", parsed.MemberId);
        Assert.Equal(8, parsed.MemberEpoch);
        Assert.Equal(5_000, parsed.HeartbeatIntervalMs);
        Assert.Equal(2, parsed.Assignment.ActiveTasks.Length);
        Assert.Single(parsed.Assignment.StandbyTasks);
        Assert.Empty(parsed.Assignment.WarmupTasks);
        Assert.Equal("0", parsed.Assignment.ActiveTasks[0].SubtopologyId);
    }

    [Fact]
    public void HeartbeatResponse_ErrorPath_RoundTrips()
    {
        var original = new StreamsGroupHeartbeatResponsePayload
        {
            ThrottleTimeMs = 0,
            ErrorCode = 110, // FENCED_MEMBER_EPOCH
            ErrorMessage = "topology epoch 3 is stale — current topology epoch is 5",
            MemberId = "instance-1",
            MemberEpoch = -1,
            HeartbeatIntervalMs = 5_000,
            Assignment = new StreamsTaskAssignment(
                Array.Empty<StreamsTaskId>(),
                Array.Empty<StreamsTaskId>(),
                Array.Empty<StreamsTaskId>()),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return StreamsGroupHeartbeatResponsePayload.Read(ref r); });

        Assert.Equal((short)110, parsed.ErrorCode);
        Assert.Contains("topology epoch", parsed.ErrorMessage);
        Assert.Equal(-1, parsed.MemberEpoch);
    }

    // ───────────────────────────────────────────────────────────────
    // StreamsGroupDescribe
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeRequest_RoundTrip_PreservesGroupIdsAndFlag()
    {
        var original = new StreamsGroupDescribeRequestPayload
        {
            GroupIds = new[] { "wordcount-app", "agg-app" },
            IncludeAuthorizedOperations = true,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return StreamsGroupDescribeRequestPayload.Read(ref r); });

        Assert.Equal(new[] { "wordcount-app", "agg-app" }, parsed.GroupIds);
        Assert.True(parsed.IncludeAuthorizedOperations);
    }

    [Fact]
    public void DescribeResponse_FullShape_RoundTrips()
    {
        var original = new StreamsGroupDescribeResponsePayload
        {
            ThrottleTimeMs = 0,
            Groups = new[]
            {
                new DescribedStreamsGroup
                {
                    ErrorCode = 0,
                    GroupId = "wordcount-app",
                    GroupState = "Stable",
                    GroupEpoch = 12,
                    TopologyEpoch = 5,
                    AssignmentEpoch = 12,
                    Members = new[]
                    {
                        new StreamsGroupMember
                        {
                            MemberId = "instance-1",
                            InstanceId = "static-1",
                            RackId = "az-a",
                            ClientId = "client-1",
                            ClientHost = "/10.0.0.1",
                            ProcessId = ProcessIdA,
                            TopologyEpoch = 5,
                            Assignment = new StreamsTaskAssignment(
                                ActiveTasks: new[] { new StreamsTaskId("0", 0) },
                                StandbyTasks: Array.Empty<StreamsTaskId>(),
                                WarmupTasks: Array.Empty<StreamsTaskId>()),
                            IsClassic = false,
                        },
                        new StreamsGroupMember
                        {
                            MemberId = "instance-2",
                            InstanceId = null,
                            RackId = null,
                            ClientId = "client-2",
                            ClientHost = "/10.0.0.2",
                            ProcessId = ProcessIdB,
                            TopologyEpoch = 5,
                            Assignment = new StreamsTaskAssignment(
                                ActiveTasks: new[] { new StreamsTaskId("0", 1) },
                                StandbyTasks: new[] { new StreamsTaskId("0", 0) },
                                WarmupTasks: Array.Empty<StreamsTaskId>()),
                            IsClassic = true, // migrated from classic protocol
                        },
                    },
                },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return StreamsGroupDescribeResponsePayload.Read(ref r); });

        Assert.Single(parsed.Groups);
        var group = parsed.Groups[0];
        Assert.Equal("wordcount-app", group.GroupId);
        Assert.Equal(5, group.TopologyEpoch);
        Assert.Equal(2, group.Members.Length);

        var m1 = group.Members[0];
        Assert.Equal("static-1", m1.InstanceId);
        Assert.Equal(ProcessIdA, m1.ProcessId);
        Assert.False(m1.IsClassic);
        Assert.Single(m1.Assignment.ActiveTasks);

        var m2 = group.Members[1];
        Assert.Null(m2.InstanceId);
        Assert.Equal(ProcessIdB, m2.ProcessId);
        Assert.True(m2.IsClassic);
        Assert.Equal("0", m2.Assignment.StandbyTasks[0].SubtopologyId);
    }

    [Fact]
    public void DescribeResponse_EmptyGroups_RoundTrips()
    {
        var original = new StreamsGroupDescribeResponsePayload
        {
            ThrottleTimeMs = 0,
            Groups = Array.Empty<DescribedStreamsGroup>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return StreamsGroupDescribeResponsePayload.Read(ref r); });
        Assert.Empty(parsed.Groups);
    }
}

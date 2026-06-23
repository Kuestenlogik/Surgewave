using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — the classic ConsumerGroups RPCs that flank
/// KIP-848 (which has its own bigger payloads in a separate batch):
/// <see cref="DescribeGroupRequestPayload"/> + Response,
/// <see cref="DeleteGroupRequestPayload"/> + Response,
/// <see cref="FindCoordinatorRequestPayload"/> + Response,
/// <see cref="HeartbeatRequestPayload"/> + Response.
///
/// All eight were sitting at 0% on the latest report — they back the
/// admin-side group introspection RPCs the Control UI uses, so framing
/// regressions show up as "empty group list" or "wrong coordinator"
/// reports in the field.
/// </summary>
public sealed class ConsumerGroupBasicPayloadRoundTripTests
{
    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // DescribeGroup
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeGroupRequest_RoundTrip_PreservesGroupId()
    {
        var original = new DescribeGroupRequestPayload { GroupId = "g-test" };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeGroupRequestPayload.Read(ref r); });
        Assert.Equal("g-test", parsed.GroupId);
    }

    [Fact]
    public void DescribeGroupResponse_RoundTrip_PreservesMembersAndMetadata()
    {
        var original = new DescribeGroupResponsePayload
        {
            ErrorCode = 0,
            GroupId = "g-test",
            State = "Stable",
            ProtocolType = "consumer",
            ProtocolName = "range",
            GenerationId = 7,
            Members = new[]
            {
                new GroupMemberPayload
                {
                    MemberId = "m1",
                    GroupInstanceId = "instance-1",
                    ClientId = "client-a",
                    Metadata = new byte[] { 1, 2, 3 },
                    Assignment = new byte[] { 4, 5 },
                },
                new GroupMemberPayload
                {
                    MemberId = "m2",
                    GroupInstanceId = null, // anonymous member
                    ClientId = "client-b",
                    Metadata = Array.Empty<byte>(),
                    Assignment = Array.Empty<byte>(),
                },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeGroupResponsePayload.Read(ref r); });

        Assert.Equal("Stable", parsed.State);
        Assert.Equal("range", parsed.ProtocolName);
        Assert.Equal(7, parsed.GenerationId);
        Assert.Equal(2, parsed.Members.Length);

        Assert.Equal("m1", parsed.Members[0].MemberId);
        Assert.Equal("instance-1", parsed.Members[0].GroupInstanceId);
        Assert.Equal(new byte[] { 1, 2, 3 }, parsed.Members[0].Metadata);
        Assert.Equal(new byte[] { 4, 5 }, parsed.Members[0].Assignment);

        Assert.Equal("m2", parsed.Members[1].MemberId);
        Assert.Null(parsed.Members[1].GroupInstanceId);
        Assert.Empty(parsed.Members[1].Metadata);
        Assert.Empty(parsed.Members[1].Assignment);
    }

    [Fact]
    public void DescribeGroupResponse_EmptyMemberList_RoundTrips()
    {
        // Dead-state group has no members; pin that the empty-list count
        // doesn't drift on the wire (int16 zero, not int32).
        var original = new DescribeGroupResponsePayload
        {
            ErrorCode = 0,
            GroupId = "g-dead",
            State = "Dead",
            ProtocolType = "",
            ProtocolName = "",
            GenerationId = -1,
            Members = Array.Empty<GroupMemberPayload>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeGroupResponsePayload.Read(ref r); });

        Assert.Equal("Dead", parsed.State);
        Assert.Empty(parsed.Members);
    }

    [Fact]
    public void DescribeGroupResponse_ErrorPath_RoundTrips()
    {
        // GROUP_AUTHORIZATION_FAILED (30) — broker rejected the request.
        var original = new DescribeGroupResponsePayload
        {
            ErrorCode = 30,
            GroupId = "g-forbidden",
            State = "",
            ProtocolType = "",
            ProtocolName = "",
            GenerationId = 0,
            Members = Array.Empty<GroupMemberPayload>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeGroupResponsePayload.Read(ref r); });
        Assert.Equal((ushort)30, parsed.ErrorCode);
    }

    // ───────────────────────────────────────────────────────────────
    // DeleteGroup
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DeleteGroupRequest_RoundTrip_PreservesGroupId()
    {
        var original = new DeleteGroupRequestPayload { GroupId = "g-to-delete" };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DeleteGroupRequestPayload.Read(ref r); });
        Assert.Equal("g-to-delete", parsed.GroupId);
    }

    [Theory]
    [InlineData((ushort)0)]   // OK
    [InlineData((ushort)68)]  // NON_EMPTY_GROUP
    [InlineData((ushort)30)]  // GROUP_AUTHORIZATION_FAILED
    public void DeleteGroupResponse_RoundTrips_AllErrorCodes(ushort errorCode)
    {
        var original = new DeleteGroupResponsePayload { ErrorCode = errorCode };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DeleteGroupResponsePayload.Read(ref r); });
        Assert.Equal(errorCode, parsed.ErrorCode);
    }

    // ───────────────────────────────────────────────────────────────
    // FindCoordinator
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void FindCoordinatorRequest_RoundTrip_PreservesKeyAndKeyType()
    {
        var original = new FindCoordinatorRequestPayload { Key = "g-some-group", KeyType = 0 };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return FindCoordinatorRequestPayload.Read(ref r); });
        Assert.Equal("g-some-group", parsed.Key);
        Assert.Equal((byte)0, parsed.KeyType);
    }

    [Fact]
    public void FindCoordinatorRequest_KeyType1_TransactionalId_RoundTrips()
    {
        // KeyType 1 = TRANSACTION (per Kafka's FindCoordinator key types).
        var original = new FindCoordinatorRequestPayload { Key = "tx-1", KeyType = 1 };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return FindCoordinatorRequestPayload.Read(ref r); });
        Assert.Equal((byte)1, parsed.KeyType);
    }

    [Fact]
    public void FindCoordinatorResponse_RoundTrip_PreservesCoordinatorEndpoint()
    {
        var original = new FindCoordinatorResponsePayload
        {
            ErrorCode = 0,
            CoordinatorId = 7,
            Host = "broker-7.kl.local",
            Port = 9092,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return FindCoordinatorResponsePayload.Read(ref r); });

        Assert.Equal(7, parsed.CoordinatorId);
        Assert.Equal("broker-7.kl.local", parsed.Host);
        Assert.Equal(9092, parsed.Port);
    }

    [Fact]
    public void FindCoordinatorResponse_CoordinatorNotAvailable_RoundTrips()
    {
        // GROUP_COORDINATOR_NOT_AVAILABLE (15) — the broker doesn't know
        // who the coordinator is yet.
        var original = new FindCoordinatorResponsePayload
        {
            ErrorCode = 15,
            CoordinatorId = -1,
            Host = "",
            Port = -1,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return FindCoordinatorResponsePayload.Read(ref r); });

        Assert.Equal((ushort)15, parsed.ErrorCode);
        Assert.Equal(-1, parsed.CoordinatorId);
        Assert.Equal("", parsed.Host);
        Assert.Equal(-1, parsed.Port);
    }

    // ───────────────────────────────────────────────────────────────
    // Heartbeat (classic v0 — not KIP-848)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void HeartbeatRequest_RoundTrip_PreservesAllFields()
    {
        var original = new HeartbeatRequestPayload
        {
            GroupId = "g-classic",
            MemberId = "consumer-1-abcd",
            GenerationId = 42,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return HeartbeatRequestPayload.Read(ref r); });

        Assert.Equal("g-classic", parsed.GroupId);
        Assert.Equal("consumer-1-abcd", parsed.MemberId);
        Assert.Equal(42, parsed.GenerationId);
    }

    [Theory]
    [InlineData((ushort)0)]   // OK
    [InlineData((ushort)22)]  // ILLEGAL_GENERATION
    [InlineData((ushort)25)]  // UNKNOWN_MEMBER_ID
    public void HeartbeatResponse_RoundTrips_AllErrorCodes(ushort errorCode)
    {
        var original = new HeartbeatResponsePayload { ErrorCode = errorCode };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return HeartbeatResponsePayload.Read(ref r); });
        Assert.Equal(errorCode, parsed.ErrorCode);
    }
}

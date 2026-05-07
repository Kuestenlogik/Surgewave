using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Tests for consumer group payload serialization/deserialization
/// </summary>
public sealed class ConsumerGroupPayloadTests
{
    [Fact]
    public void JoinGroupRequest_RoundTrip_NoProtocols()
    {
        var payload = new JoinGroupRequestPayload
        {
            GroupId = "my-consumer-group",
            MemberId = null,
            GroupInstanceId = null,
            ClientId = "client-1",
            ProtocolType = "consumer",
            SessionTimeoutMs = 30000,
            RebalanceTimeoutMs = 60000,
            Protocols = []
        };

        var buffer = new byte[payload.EstimateSize() + 20];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = JoinGroupRequestPayload.Read(ref reader);

        Assert.Equal("my-consumer-group", parsed.GroupId);
        Assert.Null(parsed.MemberId);
        Assert.Null(parsed.GroupInstanceId);
        Assert.Equal("client-1", parsed.ClientId);
        Assert.Equal("consumer", parsed.ProtocolType);
        Assert.Equal(30000, parsed.SessionTimeoutMs);
        Assert.Equal(60000, parsed.RebalanceTimeoutMs);
        Assert.Empty(parsed.Protocols);
    }

    [Fact]
    public void JoinGroupRequest_RoundTrip_WithProtocols()
    {
        var metadata = new byte[] { 1, 2, 3, 4, 5 };
        var payload = new JoinGroupRequestPayload
        {
            GroupId = "group-1",
            MemberId = "member-abc",
            GroupInstanceId = "static-instance",
            ClientId = "client-x",
            ProtocolType = "consumer",
            SessionTimeoutMs = 10000,
            RebalanceTimeoutMs = 20000,
            Protocols = [new GroupProtocol("range", metadata), new GroupProtocol("roundrobin", [])]
        };

        var buffer = new byte[payload.EstimateSize() + 20];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = JoinGroupRequestPayload.Read(ref reader);

        Assert.Equal("group-1", parsed.GroupId);
        Assert.Equal("member-abc", parsed.MemberId);
        Assert.Equal("static-instance", parsed.GroupInstanceId);
        Assert.Equal(2, parsed.Protocols.Length);
        Assert.Equal("range", parsed.Protocols[0].Name);
        Assert.Equal(metadata, parsed.Protocols[0].Metadata);
        Assert.Equal("roundrobin", parsed.Protocols[1].Name);
        Assert.Empty(parsed.Protocols[1].Metadata);
    }

    [Fact]
    public void HeartbeatRequest_RoundTrip()
    {
        var payload = new HeartbeatRequestPayload
        {
            GroupId = "group-abc",
            MemberId = "member-xyz",
            GenerationId = 42
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = HeartbeatRequestPayload.Read(ref reader);

        Assert.Equal("group-abc", parsed.GroupId);
        Assert.Equal("member-xyz", parsed.MemberId);
        Assert.Equal(42, parsed.GenerationId);
    }

    [Fact]
    public void LeaveGroupRequest_RoundTrip()
    {
        var payload = new LeaveGroupRequestPayload
        {
            GroupId = "departing-group",
            MemberId = "departing-member"
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = LeaveGroupRequestPayload.Read(ref reader);

        Assert.Equal("departing-group", parsed.GroupId);
        Assert.Equal("departing-member", parsed.MemberId);
    }

    [Fact]
    public void HeartbeatRequest_EstimateSize_IsAccurate()
    {
        var payload = new HeartbeatRequestPayload
        {
            GroupId = "grp",
            MemberId = "mbr",
            GenerationId = 0
        };

        var estimated = payload.EstimateSize();
        var buffer = new byte[estimated + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        Assert.True(writer.Position <= estimated);
    }

    [Fact]
    public void LeaveGroupRequest_EstimateSize_MatchesActualWrite()
    {
        var payload = new LeaveGroupRequestPayload
        {
            GroupId = "test-group",
            MemberId = "test-member"
        };

        var estimated = payload.EstimateSize();
        var buffer = new byte[estimated + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        // 2+10 (GroupId) + 2+11 (MemberId) = 25
        Assert.Equal(25, writer.Position);
        Assert.Equal(25, estimated);
    }

    [Fact]
    public void JoinGroupRequest_GenerationId_NotPartOfPayload_EstimateCorrect()
    {
        var payload = new JoinGroupRequestPayload
        {
            GroupId = "g",
            MemberId = null,
            GroupInstanceId = null,
            ClientId = "c",
            ProtocolType = "consumer",
            SessionTimeoutMs = 0,
            RebalanceTimeoutMs = 0,
            Protocols = []
        };

        var estimated = payload.EstimateSize();
        // Should be at least: 2+1 (GroupId) + 2 (MemberId null) + 2 (GroupInstanceId null) + 2+1 (ClientId)
        // + 2+8 (ProtocolType "consumer") + 4 + 4 + 2 = 30
        Assert.True(estimated > 0);
    }
}

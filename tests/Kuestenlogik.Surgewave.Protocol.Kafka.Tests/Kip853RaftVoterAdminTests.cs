using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Coverage-push batch — KIP-853 (Dynamic Raft Voter Reconfiguration)
/// admin RPCs. Covers
/// <see cref="AddRaftVoterRequest"/> + Response (API key 80, v0-1),
/// <see cref="RemoveRaftVoterRequest"/> + Response (API key 81, v0), and
/// <see cref="UpdateRaftVoterRequest"/> + Response (API key 82, v0).
///
/// Per kips.md, KIP-853 in Surgewave is "Wire bound + foundation in
/// place — semantics not implemented". RPCs 80/81/82 advertise and
/// bind to a polite-rejection handler. The wire pin is therefore the
/// load-bearing part of the conformance story for this KIP: it
/// guarantees a v2 librdkafka admin client can send the RPC and parse
/// the response, even while the broker rejects it.
///
/// v1 of AddRaftVoter added the <c>AckWhenCommitted</c> flag (KIP-1186);
/// both v0 (default true) and v1 paths get pinned.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip853RaftVoterAdminTests
{
    private static readonly Guid VoterDirectoryId = new("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

    private static KafkaProtocolReader SkipFlexibleHeader(byte[] payload)
    {
        var reader = new KafkaProtocolReader(payload);
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadCompactString(); reader.SkipTaggedFields();
        return reader;
    }

    // ───────────────────────────────────────────────────────────────
    // AddRaftVoter (API key 80, v0-1)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void AddRequest_V0_NoAckFlag_RoundTrips()
    {
        // v0 doesn't carry AckWhenCommitted — Read always returns the default (true).
        var original = new AddRaftVoterRequest
        {
            ApiKey = ApiKey.AddRaftVoter,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "raft-admin",
            ClusterId = "surgewave-quorum",
            TimeoutMs = 30_000,
            VoterId = 4,
            VoterDirectoryId = VoterDirectoryId,
            Listeners =
            [
                new AddRaftVoterRequest.ListenerInfo { Name = "CONTROLLER", Host = "voter-4.svc", Port = 9093 },
            ],
            AckWhenCommitted = false, // set on model but should NOT survive at v0
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AddRaftVoterRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "raft-admin");

        Assert.Equal("surgewave-quorum", parsed.ClusterId);
        Assert.Equal(4, parsed.VoterId);
        Assert.Equal(VoterDirectoryId, parsed.VoterDirectoryId);
        Assert.Single(parsed.Listeners);
        Assert.Equal("CONTROLLER", parsed.Listeners[0].Name);
        Assert.Equal((ushort)9093, parsed.Listeners[0].Port);
        // v0 default — AckWhenCommitted not on wire, Read returns true regardless of model.
        Assert.True(parsed.AckWhenCommitted);
    }

    [Fact]
    public void AddRequest_V1_AckFlagFalse_RoundTrips()
    {
        // KIP-1186 — v1 added AckWhenCommitted. False means "respond
        // immediately after accepting, don't wait for commit".
        var original = new AddRaftVoterRequest
        {
            ApiKey = ApiKey.AddRaftVoter,
            ApiVersion = 1,
            CorrelationId = 1,
            ClientId = "raft-admin",
            ClusterId = "surgewave-quorum",
            TimeoutMs = 30_000,
            VoterId = 5,
            VoterDirectoryId = VoterDirectoryId,
            Listeners =
            [
                new AddRaftVoterRequest.ListenerInfo { Name = "CONTROLLER",      Host = "voter-5.svc",   Port = 9093 },
                new AddRaftVoterRequest.ListenerInfo { Name = "CONTROLLER_TLS",  Host = "voter-5.tls",   Port = 9094 },
            ],
            AckWhenCommitted = false,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AddRaftVoterRequest.ReadFrom(reader, apiVersion: 1, correlationId: 1, clientId: "raft-admin");

        Assert.Equal(2, parsed.Listeners.Count);
        Assert.Equal("CONTROLLER_TLS", parsed.Listeners[1].Name);
        Assert.Equal((ushort)9094, parsed.Listeners[1].Port);
        Assert.False(parsed.AckWhenCommitted);
    }

    [Fact]
    public void AddResponse_Success_RoundTrips()
    {
        var original = new AddRaftVoterResponse
        {
            ApiVersion = 1,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AddRaftVoterResponse.ReadFrom(reader, apiVersion: 1, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Null(parsed.ErrorMessage);
    }

    [Fact]
    public void AddResponse_PoliteRejection_RoundTrips()
    {
        // Per kips.md: Surgewave's KIP-853 binding rejects with
        // InvalidRequest (42). Pin that the ErrorMessage propagates.
        var original = new AddRaftVoterResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.InvalidRequest,
            ErrorMessage = "Dynamic Raft voter changes not yet implemented",
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AddRaftVoterResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.InvalidRequest, parsed.ErrorCode);
        Assert.Contains("not yet implemented", parsed.ErrorMessage);
    }

    // ───────────────────────────────────────────────────────────────
    // RemoveRaftVoter (API key 81, v0)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void RemoveRequest_RoundTrip_PreservesAllFields()
    {
        var original = new RemoveRaftVoterRequest
        {
            ApiKey = ApiKey.RemoveRaftVoter,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "raft-admin",
            ClusterId = "surgewave-quorum",
            VoterId = 4,
            VoterDirectoryId = VoterDirectoryId,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = RemoveRaftVoterRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "raft-admin");

        Assert.Equal("surgewave-quorum", parsed.ClusterId);
        Assert.Equal(4, parsed.VoterId);
        Assert.Equal(VoterDirectoryId, parsed.VoterDirectoryId);
    }

    [Fact]
    public void RemoveResponse_RoundTrip_PreservesAllFields()
    {
        var original = new RemoveRaftVoterResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = RemoveRaftVoterResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
    }

    // ───────────────────────────────────────────────────────────────
    // UpdateRaftVoter (API key 82, v0)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void UpdateRequest_FullShape_RoundTrips()
    {
        var original = new UpdateRaftVoterRequest
        {
            ApiKey = ApiKey.UpdateRaftVoter,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "raft-admin",
            ClusterId = "surgewave-quorum",
            CurrentLeaderEpoch = 7,
            VoterId = 4,
            VoterDirectoryId = VoterDirectoryId,
            Listeners =
            [
                new UpdateRaftVoterRequest.ListenerInfo { Name = "CONTROLLER", Host = "leader.svc", Port = 9093 },
            ],
            KRaftVersionFeature = new UpdateRaftVoterRequest.KRaftVersionFeatureInfo
            {
                MinSupportedVersion = 0,
                MaxSupportedVersion = 1,
            },
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = UpdateRaftVoterRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "raft-admin");

        Assert.Equal("surgewave-quorum", parsed.ClusterId);
        Assert.Equal(7, parsed.CurrentLeaderEpoch);
        Assert.Equal(4, parsed.VoterId);
        Assert.Equal(VoterDirectoryId, parsed.VoterDirectoryId);
        Assert.Single(parsed.Listeners);
        Assert.Equal("leader.svc", parsed.Listeners[0].Host);
        Assert.Equal((short)0, parsed.KRaftVersionFeature.MinSupportedVersion);
        Assert.Equal((short)1, parsed.KRaftVersionFeature.MaxSupportedVersion);
    }

    [Fact]
    public void UpdateRequest_UnknownLeaderEpoch_RoundTrips()
    {
        // CurrentLeaderEpoch=-1 = "I don't know who the leader is".
        var original = new UpdateRaftVoterRequest
        {
            ApiKey = ApiKey.UpdateRaftVoter,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "raft-admin",
            ClusterId = "surgewave-quorum",
            CurrentLeaderEpoch = -1,
            VoterId = 4,
            VoterDirectoryId = VoterDirectoryId,
            Listeners = [],
            KRaftVersionFeature = new UpdateRaftVoterRequest.KRaftVersionFeatureInfo
            {
                MinSupportedVersion = 0,
                MaxSupportedVersion = 1,
            },
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = UpdateRaftVoterRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "raft-admin");

        Assert.Equal(-1, parsed.CurrentLeaderEpoch);
        Assert.Empty(parsed.Listeners);
    }

    [Fact]
    public void UpdateResponse_NoCurrentLeader_RoundTrips()
    {
        var original = new UpdateRaftVoterResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            CurrentLeader = null, // tagged field absent
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = UpdateRaftVoterResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Null(parsed.CurrentLeader);
    }

    [Fact]
    public void UpdateResponse_WithCurrentLeaderTaggedField_ReadDropsIt()
    {
        // The CurrentLeader tagged field (tag 0) is emitted by WriteTo but
        // the current ReadFrom implementation skips all tagged fields rather
        // than parsing tag 0. The Write side stays exercised; the Read
        // produces CurrentLeader=null. Pinning this asymmetry rather than
        // a "spec-ideal" round-trip — see RaftVoterRequests.cs:469-471 for
        // the deliberate simplification comment.
        var original = new UpdateRaftVoterResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            CurrentLeader = new UpdateRaftVoterResponse.CurrentLeaderInfo
            {
                LeaderId = 1,
                LeaderEpoch = 7,
                Host = "leader.svc",
                Port = 9093,
            },
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();
        // Sanity-check: the tagged field made it onto the wire (tagged-fields
        // varint at offset CorrelationId(4) + header tag-varint(1) + ThrottleTime(4)
        // + ErrorCode(2) = 11 must be 1, not 0).
        Assert.Equal((byte)1, bytes[11]); // tag-count varint = 1

        var reader = new KafkaProtocolReader(bytes.AsSpan(4).ToArray());
        var parsed = UpdateRaftVoterResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        // Documented one-way limitation: Read skips the tag.
        Assert.Null(parsed.CurrentLeader);
    }
}

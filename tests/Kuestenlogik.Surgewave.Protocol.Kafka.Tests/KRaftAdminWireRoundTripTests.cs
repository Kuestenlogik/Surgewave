using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Coverage-push batch — KRaft-mode inter-broker RPCs in Protocol.Kafka.
/// Covers <see cref="AllocateProducerIdsRequest"/> + Response,
/// <see cref="BrokerHeartbeatRequest"/> + Response (with the v1
/// OfflineLogDirs tagged field and the v2 CordonedLogDirs nullable
/// tagged field per KAFKA-20441), and
/// <see cref="BrokerRegistrationRequest"/> + Response (with v0/v1/v2/v3
/// shape variants).
///
/// These RPCs are emitted only by brokers talking to the active
/// controller, so they're invisible to client integration tests —
/// without unit-level wire pins, framing regressions can't be caught
/// until a cluster-bringup smoke test runs.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class KRaftAdminWireRoundTripTests
{
    private static readonly Guid IncarnationId = new("11111111-2222-3333-4444-555555555555");
    private static readonly Guid LogDirA = new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid LogDirB = new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    // ───────────────────────────────────────────────────────────────
    // AllocateProducerIds (API key 67)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void AllocateProducerIdsRequest_RoundTrip_PreservesBrokerIdAndEpoch()
    {
        var original = new AllocateProducerIdsRequest
        {
            ApiKey = ApiKey.AllocateProducerIds,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "broker-7",
            BrokerId = 7,
            BrokerEpoch = 42L,
        };

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        // WriteTo emits [ApiKey(2)][ApiVersion(2)][CorrelationId(4)]
        // [ClientId compact-string][header tagged-fields varint(=0)]
        // before the body. ReadFrom expects a KafkaProtocolReader at the
        // body — slice the header off by walking past it.
        var reader = new KafkaProtocolReader(writer.ToArray());
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadCompactString(); reader.SkipTaggedFields();
        var parsed = AllocateProducerIdsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "broker-7");

        Assert.Equal(7, parsed.BrokerId);
        Assert.Equal(42L, parsed.BrokerEpoch);
    }

    [Fact]
    public void AllocateProducerIdsResponse_RoundTrip_PreservesAllocatedRange()
    {
        var original = new AllocateProducerIdsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ProducerIdStart = 1_000_000L,
            ProducerIdLen = 1_000,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        // Response.WriteTo emits CorrelationId(4) ahead of the body;
        // ReadFrom expects the reader at the response-header tagged-fields
        // varint. Slice 4 bytes off.
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AllocateProducerIdsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Equal(1_000_000L, parsed.ProducerIdStart);
        Assert.Equal(1_000, parsed.ProducerIdLen);
    }

    [Fact]
    public void AllocateProducerIdsResponse_ErrorPath_RoundTrips()
    {
        var original = new AllocateProducerIdsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.NotController,
            ProducerIdStart = -1,
            ProducerIdLen = 0,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AllocateProducerIdsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);
        Assert.Equal(ErrorCode.NotController, parsed.ErrorCode);
        Assert.Equal(-1L, parsed.ProducerIdStart);
    }

    // ───────────────────────────────────────────────────────────────
    // BrokerHeartbeat (API key 63, v0-v2)
    // ───────────────────────────────────────────────────────────────

    private static T RoundTripRequest<T>(KafkaRequest original, short apiVersion, int correlationId, string clientId,
        Func<KafkaProtocolReader, short, int, string, T> read)
    {
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray());
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadCompactString(); reader.SkipTaggedFields();
        return read(reader, apiVersion, correlationId, clientId);
    }

    [Fact]
    public void BrokerHeartbeatRequest_V0_NoTaggedFields_RoundTrips()
    {
        var original = new BrokerHeartbeatRequest
        {
            ApiKey = ApiKey.BrokerHeartbeat,
            ApiVersion = 0,
            CorrelationId = 5,
            ClientId = "broker-3",
            BrokerId = 3,
            BrokerEpoch = 17L,
            CurrentMetadataOffset = 1_000_000L,
            WantFence = false,
            WantShutDown = false,
            // v0 doesn't carry OfflineLogDirs / CordonedLogDirs — set to non-empty
            // to verify they don't appear on the wire.
            OfflineLogDirs = [LogDirA],
            CordonedLogDirs = [LogDirB],
        };

        var parsed = RoundTripRequest(original, 0, 5, "broker-3", BrokerHeartbeatRequest.ReadFrom);

        Assert.Equal(3, parsed.BrokerId);
        Assert.Equal(17L, parsed.BrokerEpoch);
        Assert.Equal(1_000_000L, parsed.CurrentMetadataOffset);
        Assert.False(parsed.WantFence);
        Assert.False(parsed.WantShutDown);
        // v0 doesn't read OfflineLogDirs — should come back empty
        Assert.Empty(parsed.OfflineLogDirs);
        Assert.Null(parsed.CordonedLogDirs);
    }

    [Fact]
    public void BrokerHeartbeatRequest_V1_OfflineLogDirsTag_RoundTrips()
    {
        var original = new BrokerHeartbeatRequest
        {
            ApiKey = ApiKey.BrokerHeartbeat,
            ApiVersion = 1,
            CorrelationId = 5,
            ClientId = "broker-3",
            BrokerId = 3,
            BrokerEpoch = 17L,
            CurrentMetadataOffset = 2_000_000L,
            WantFence = true,
            WantShutDown = false,
            OfflineLogDirs = [LogDirA, LogDirB],
            CordonedLogDirs = null, // v1 doesn't carry it
        };
        var parsed = RoundTripRequest(original, 1, 5, "broker-3", BrokerHeartbeatRequest.ReadFrom);

        Assert.True(parsed.WantFence);
        Assert.Equal(2, parsed.OfflineLogDirs.Count);
        Assert.Equal(LogDirA, parsed.OfflineLogDirs[0]);
        Assert.Equal(LogDirB, parsed.OfflineLogDirs[1]);
        Assert.Null(parsed.CordonedLogDirs); // v1 reader never sets it
    }

    [Fact]
    public void BrokerHeartbeatRequest_V2_CordonedLogDirsTag_RoundTrips()
    {
        // KAFKA-20441: CordonedLogDirs is sent only after RECOVERY state.
        // Pin both the populated case and the null case (= not yet sent).
        var withCordoned = new BrokerHeartbeatRequest
        {
            ApiKey = ApiKey.BrokerHeartbeat,
            ApiVersion = 2,
            CorrelationId = 5,
            ClientId = "broker-3",
            BrokerId = 3,
            BrokerEpoch = 17L,
            CurrentMetadataOffset = 3_000_000L,
            WantFence = false,
            WantShutDown = true,
            OfflineLogDirs = [LogDirA],
            CordonedLogDirs = [LogDirB],
        };
        var parsed = RoundTripRequest(withCordoned, 2, 5, "broker-3", BrokerHeartbeatRequest.ReadFrom);

        Assert.True(parsed.WantShutDown);
        Assert.Single(parsed.OfflineLogDirs);
        Assert.NotNull(parsed.CordonedLogDirs);
        Assert.Single(parsed.CordonedLogDirs!);
        Assert.Equal(LogDirB, parsed.CordonedLogDirs![0]);
    }

    [Fact]
    public void BrokerHeartbeatRequest_V2_NullCordonedLogDirs_TagOmitted()
    {
        // CordonedLogDirs=null at v2 means the tag MUST NOT be written
        // (pre-RECOVERY broker). Pin that the round-trip recovers null.
        var withoutCordoned = new BrokerHeartbeatRequest
        {
            ApiKey = ApiKey.BrokerHeartbeat,
            ApiVersion = 2,
            CorrelationId = 5,
            ClientId = "broker-3",
            BrokerId = 3,
            BrokerEpoch = 17L,
            CurrentMetadataOffset = 3_000_000L,
            WantFence = false,
            WantShutDown = false,
            OfflineLogDirs = [],
            CordonedLogDirs = null,
        };
        var parsed = RoundTripRequest(withoutCordoned, 2, 5, "broker-3", BrokerHeartbeatRequest.ReadFrom);

        Assert.Empty(parsed.OfflineLogDirs);
        Assert.Null(parsed.CordonedLogDirs);
    }

    [Fact]
    public void BrokerHeartbeatResponse_RoundTrip_PreservesAllFlags()
    {
        var original = new BrokerHeartbeatResponse
        {
            ApiVersion = 2,
            CorrelationId = 5,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            IsCaughtUp = true,
            IsFenced = false,
            ShouldShutDown = false,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = BrokerHeartbeatResponse.ReadFrom(reader, apiVersion: 2, correlationId: 5);

        Assert.True(parsed.IsCaughtUp);
        Assert.False(parsed.IsFenced);
        Assert.False(parsed.ShouldShutDown);
    }

    [Fact]
    public void BrokerHeartbeatResponse_FencedShape_RoundTrips()
    {
        // Controller is telling the broker it's fenced — flags shape the
        // broker's reaction to leadership reassignment.
        var original = new BrokerHeartbeatResponse
        {
            ApiVersion = 1,
            CorrelationId = 5,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            IsCaughtUp = false,
            IsFenced = true,
            ShouldShutDown = false,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = BrokerHeartbeatResponse.ReadFrom(reader, apiVersion: 1, correlationId: 5);

        Assert.False(parsed.IsCaughtUp);
        Assert.True(parsed.IsFenced);
    }

    // ───────────────────────────────────────────────────────────────
    // BrokerRegistration (API key 62, v0-v3)
    // ───────────────────────────────────────────────────────────────

    private static BrokerRegistrationRequest NewRegistration(short apiVersion, Action<BrokerRegistrationRequest>? customize = null)
    {
        var req = new BrokerRegistrationRequest
        {
            ApiKey = ApiKey.BrokerRegistration,
            ApiVersion = apiVersion,
            CorrelationId = 1,
            ClientId = "broker-3",
            BrokerId = 3,
            ClusterId = "surgewave-cluster",
            IncarnationId = IncarnationId,
            Listeners =
            [
                new BrokerRegistrationRequest.Listener
                {
                    Name = "PLAINTEXT",
                    Host = "broker-3.svc.local",
                    Port = 9092,
                    SecurityProtocol = 0,
                },
            ],
            Features =
            [
                new BrokerRegistrationRequest.Feature { Name = "metadata.version", MinSupportedVersion = 1, MaxSupportedVersion = 17 },
            ],
            Rack = "az-eu-west-1a",
        };
        customize?.Invoke(req);
        return req;
    }

    [Fact]
    public void BrokerRegistrationRequest_V0_RoundTrips_NoV1PlusFields()
    {
        var original = NewRegistration(0);
        // V0 doesn't carry IsMigratingZkBroker / LogDirs / PreviousBrokerEpoch
        // on the wire even if set on the model.
        var parsed = RoundTripRequest(original, 0, 1, "broker-3", BrokerRegistrationRequest.ReadFrom);

        Assert.Equal("surgewave-cluster", parsed.ClusterId);
        Assert.Equal(IncarnationId, parsed.IncarnationId);
        Assert.Single(parsed.Listeners);
        Assert.Equal("broker-3.svc.local", parsed.Listeners[0].Host);
        Assert.Equal((ushort)9092, parsed.Listeners[0].Port);
        Assert.Single(parsed.Features);
        Assert.Equal("metadata.version", parsed.Features[0].Name);
        Assert.Equal("az-eu-west-1a", parsed.Rack);
        Assert.False(parsed.IsMigratingZkBroker); // v0 default
        Assert.Empty(parsed.LogDirs);
        Assert.Equal(-1L, parsed.PreviousBrokerEpoch);
    }

    [Fact]
    public void BrokerRegistrationRequest_V2_WithLogDirs_RoundTrips()
    {
        var original = new BrokerRegistrationRequest
        {
            ApiKey = ApiKey.BrokerRegistration,
            ApiVersion = 2,
            CorrelationId = 1,
            ClientId = "broker-3",
            BrokerId = 3,
            ClusterId = "surgewave-cluster",
            IncarnationId = IncarnationId,
            Listeners =
            [
                new BrokerRegistrationRequest.Listener { Name = "PLAINTEXT", Host = "h", Port = 9092, SecurityProtocol = 0 },
            ],
            Features =
            [
                new BrokerRegistrationRequest.Feature { Name = "metadata.version", MinSupportedVersion = 1, MaxSupportedVersion = 17 },
            ],
            Rack = null,
            IsMigratingZkBroker = false,
            LogDirs = [LogDirA, LogDirB],
        };
        var parsed = RoundTripRequest(original, 2, 1, "broker-3", BrokerRegistrationRequest.ReadFrom);

        Assert.Null(parsed.Rack);
        Assert.Equal(2, parsed.LogDirs.Count);
        Assert.Equal(LogDirA, parsed.LogDirs[0]);
        Assert.Equal(LogDirB, parsed.LogDirs[1]);
    }

    [Fact]
    public void BrokerRegistrationRequest_V3_WithPreviousBrokerEpoch_RoundTrips()
    {
        var original = new BrokerRegistrationRequest
        {
            ApiKey = ApiKey.BrokerRegistration,
            ApiVersion = 3,
            CorrelationId = 1,
            ClientId = "broker-3",
            BrokerId = 3,
            ClusterId = "surgewave-cluster",
            IncarnationId = IncarnationId,
            Listeners =
            [
                new BrokerRegistrationRequest.Listener { Name = "PLAINTEXT", Host = "h", Port = 9092, SecurityProtocol = 0 },
            ],
            Features = [],
            Rack = null,
            LogDirs = [LogDirA],
            PreviousBrokerEpoch = 99L, // clean-shutdown epoch
        };
        var parsed = RoundTripRequest(original, 3, 1, "broker-3", BrokerRegistrationRequest.ReadFrom);

        Assert.Equal(99L, parsed.PreviousBrokerEpoch);
    }

    [Fact]
    public void BrokerRegistrationResponse_RoundTrip_AssignsBrokerEpoch()
    {
        var original = new BrokerRegistrationResponse
        {
            ApiVersion = 3,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            BrokerEpoch = 100L,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = BrokerRegistrationResponse.ReadFrom(reader, apiVersion: 3, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Equal(100L, parsed.BrokerEpoch);
    }

    [Fact]
    public void BrokerRegistrationResponse_RejectionShape_RoundTrips()
    {
        // INCONSISTENT_CLUSTER_ID — controller refused the broker. BrokerEpoch
        // stays at -1 so the broker knows registration failed.
        var original = new BrokerRegistrationResponse
        {
            ApiVersion = 3,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.InconsistentClusterId,
            BrokerEpoch = -1L,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = BrokerRegistrationResponse.ReadFrom(reader, apiVersion: 3, correlationId: 1);

        Assert.Equal(ErrorCode.InconsistentClusterId, parsed.ErrorCode);
        Assert.Equal(-1L, parsed.BrokerEpoch);
    }
}

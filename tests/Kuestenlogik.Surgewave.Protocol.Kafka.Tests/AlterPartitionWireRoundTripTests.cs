using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Coverage-push batch — AlterPartition (API key 56, v0-3). The
/// inter-broker ISR-change RPC: leader brokers send this to the
/// controller when the ISR shrinks or grows so the controller can
/// publish the new metadata to followers.
///
/// AlterPartition has the widest version matrix of any single Kafka
/// RPC: v0 baseline, v1 added LeaderRecoveryState mechanics, v2
/// switched topic identity from name to UUID (TopicId), v3 replaced
/// the flat NewIsr/Isr int-array with NewIsrWithEpochs/IsrWithEpochs
/// (per-broker BrokerState carrying broker-id + broker-epoch). Every
/// version boundary changes the wire shape; all four get round-trip
/// pins so the WriteTo/ReadFrom version branching stays honest.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class AlterPartitionWireRoundTripTests
{
    private static readonly Guid TopicIdA = new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid TopicIdB = new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    private static KafkaProtocolReader SkipFlexibleHeader(byte[] payload)
    {
        var reader = new KafkaProtocolReader(payload);
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadCompactString(); reader.SkipTaggedFields();
        return reader;
    }

    // ───────────────────────────────────────────────────────────────
    // Request (v0-3) — topic identity + ISR shape vary by version
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void Request_V0_TopicName_FlatNewIsr_RoundTrips()
    {
        // v0: topic identified by Name, ISR is a flat int[].
        var original = new AlterPartitionRequest
        {
            ApiKey = ApiKey.AlterPartition,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "broker-1",
            BrokerId = 1,
            BrokerEpoch = 17L,
            Topics =
            [
                new AlterPartitionRequest.TopicData
                {
                    TopicName = "orders",
                    Partitions =
                    [
                        new AlterPartitionRequest.PartitionData
                        {
                            PartitionIndex = 0,
                            LeaderEpoch = 5,
                            NewIsr = [1, 2, 3],         // current ISR
                            LeaderRecoveryState = 0,    // RECOVERED
                            PartitionEpoch = 42,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterPartitionRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "broker-1");

        Assert.Equal(1, parsed.BrokerId);
        Assert.Equal(17L, parsed.BrokerEpoch);
        Assert.Single(parsed.Topics);
        Assert.Equal("orders", parsed.Topics[0].TopicName);
        Assert.Equal(Guid.Empty, parsed.Topics[0].TopicId); // v0 doesn't carry UUID

        var p = parsed.Topics[0].Partitions[0];
        Assert.Equal(5, p.LeaderEpoch);
        Assert.Equal(new[] { 1, 2, 3 }, p.NewIsr);
        Assert.Null(p.NewIsrWithEpochs); // v0 path doesn't populate
        Assert.Equal(0, p.LeaderRecoveryState);
        Assert.Equal(42, p.PartitionEpoch);
    }

    [Fact]
    public void Request_V1_LeaderRecoveryStateRecovering_RoundTrips()
    {
        // v1 introduced LeaderRecoveryState=1 (RECOVERING) for the
        // post-unclean-election state. Wire shape unchanged from v0,
        // semantics expanded.
        var original = new AlterPartitionRequest
        {
            ApiKey = ApiKey.AlterPartition,
            ApiVersion = 1,
            CorrelationId = 1,
            ClientId = "broker-1",
            BrokerId = 1,
            BrokerEpoch = 17L,
            Topics =
            [
                new AlterPartitionRequest.TopicData
                {
                    TopicName = "orders",
                    Partitions =
                    [
                        new AlterPartitionRequest.PartitionData
                        {
                            PartitionIndex = 0,
                            LeaderEpoch = 5,
                            NewIsr = [1], // shrunk to just the new leader
                            LeaderRecoveryState = 1, // RECOVERING
                            PartitionEpoch = 43,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterPartitionRequest.ReadFrom(reader, apiVersion: 1, correlationId: 1, clientId: "broker-1");

        Assert.Equal(1, parsed.Topics[0].Partitions[0].LeaderRecoveryState);
        Assert.Equal(new[] { 1 }, parsed.Topics[0].Partitions[0].NewIsr);
    }

    [Fact]
    public void Request_V2_TopicIdInsteadOfName_RoundTrips()
    {
        // v2 switched topic identity from name to UUID.
        var original = new AlterPartitionRequest
        {
            ApiKey = ApiKey.AlterPartition,
            ApiVersion = 2,
            CorrelationId = 1,
            ClientId = "broker-1",
            BrokerId = 1,
            BrokerEpoch = 17L,
            Topics =
            [
                new AlterPartitionRequest.TopicData
                {
                    TopicName = "ignored-at-v2", // wire writes TopicId, not Name
                    TopicId = TopicIdA,
                    Partitions =
                    [
                        new AlterPartitionRequest.PartitionData
                        {
                            PartitionIndex = 0,
                            LeaderEpoch = 5,
                            NewIsr = [1, 2],
                            LeaderRecoveryState = 0,
                            PartitionEpoch = 42,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterPartitionRequest.ReadFrom(reader, apiVersion: 2, correlationId: 1, clientId: "broker-1");

        Assert.Equal(TopicIdA, parsed.Topics[0].TopicId);
        Assert.Null(parsed.Topics[0].TopicName); // v2 wire doesn't carry the name
        Assert.Equal(new[] { 1, 2 }, parsed.Topics[0].Partitions[0].NewIsr);
    }

    [Fact]
    public void Request_V3_NewIsrWithEpochs_RoundTrips()
    {
        // v3 replaced the flat int[] NewIsr with BrokerState[] carrying
        // broker-id + broker-epoch. Catches the case where a broker re-
        // joins the ISR before the controller has noticed its epoch bump.
        var original = new AlterPartitionRequest
        {
            ApiKey = ApiKey.AlterPartition,
            ApiVersion = 3,
            CorrelationId = 1,
            ClientId = "broker-1",
            BrokerId = 1,
            BrokerEpoch = 17L,
            Topics =
            [
                new AlterPartitionRequest.TopicData
                {
                    TopicId = TopicIdA,
                    Partitions =
                    [
                        new AlterPartitionRequest.PartitionData
                        {
                            PartitionIndex = 0,
                            LeaderEpoch = 5,
                            NewIsrWithEpochs =
                            [
                                new AlterPartitionRequest.BrokerState { BrokerId = 1, BrokerEpoch = 17L },
                                new AlterPartitionRequest.BrokerState { BrokerId = 2, BrokerEpoch = 23L },
                                new AlterPartitionRequest.BrokerState { BrokerId = 3, BrokerEpoch = 19L },
                            ],
                            LeaderRecoveryState = 0,
                            PartitionEpoch = 42,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterPartitionRequest.ReadFrom(reader, apiVersion: 3, correlationId: 1, clientId: "broker-1");

        var p = parsed.Topics[0].Partitions[0];
        Assert.Null(p.NewIsr); // v3 path doesn't populate the flat list
        Assert.NotNull(p.NewIsrWithEpochs);
        Assert.Equal(3, p.NewIsrWithEpochs!.Count);
        Assert.Equal(1, p.NewIsrWithEpochs[0].BrokerId);
        Assert.Equal(17L, p.NewIsrWithEpochs[0].BrokerEpoch);
        Assert.Equal(2, p.NewIsrWithEpochs[1].BrokerId);
        Assert.Equal(23L, p.NewIsrWithEpochs[1].BrokerEpoch);
        Assert.Equal(19L, p.NewIsrWithEpochs[2].BrokerEpoch);
    }

    [Fact]
    public void Request_V3_MultipleTopicsMultiplePartitions_RoundTrips()
    {
        // Realistic shape: leader broker reports ISR changes across
        // multiple topics in one round-trip.
        var original = new AlterPartitionRequest
        {
            ApiKey = ApiKey.AlterPartition,
            ApiVersion = 3,
            CorrelationId = 1,
            ClientId = "broker-1",
            BrokerId = 1,
            BrokerEpoch = 17L,
            Topics =
            [
                new AlterPartitionRequest.TopicData
                {
                    TopicId = TopicIdA,
                    Partitions =
                    [
                        new AlterPartitionRequest.PartitionData
                        {
                            PartitionIndex = 0, LeaderEpoch = 5,
                            NewIsrWithEpochs = [new AlterPartitionRequest.BrokerState { BrokerId = 1, BrokerEpoch = 17 }],
                            LeaderRecoveryState = 0, PartitionEpoch = 42,
                        },
                        new AlterPartitionRequest.PartitionData
                        {
                            PartitionIndex = 1, LeaderEpoch = 5,
                            NewIsrWithEpochs = [
                                new AlterPartitionRequest.BrokerState { BrokerId = 1, BrokerEpoch = 17 },
                                new AlterPartitionRequest.BrokerState { BrokerId = 2, BrokerEpoch = 23 },
                            ],
                            LeaderRecoveryState = 0, PartitionEpoch = 42,
                        },
                    ],
                },
                new AlterPartitionRequest.TopicData
                {
                    TopicId = TopicIdB,
                    Partitions =
                    [
                        new AlterPartitionRequest.PartitionData
                        {
                            PartitionIndex = 0, LeaderEpoch = 3,
                            NewIsrWithEpochs = [],   // empty ISR-with-epochs is valid (leader alone post-fence)
                            LeaderRecoveryState = 1,
                            PartitionEpoch = 7,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterPartitionRequest.ReadFrom(reader, apiVersion: 3, correlationId: 1, clientId: "broker-1");

        Assert.Equal(2, parsed.Topics.Count);
        Assert.Equal(TopicIdA, parsed.Topics[0].TopicId);
        Assert.Equal(2, parsed.Topics[0].Partitions.Count);
        Assert.Equal(2, parsed.Topics[0].Partitions[1].NewIsrWithEpochs!.Count);
        Assert.Equal(TopicIdB, parsed.Topics[1].TopicId);
        Assert.Empty(parsed.Topics[1].Partitions[0].NewIsrWithEpochs!);
        Assert.Equal(1, parsed.Topics[1].Partitions[0].LeaderRecoveryState);
    }

    // ───────────────────────────────────────────────────────────────
    // Response (v0-3)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void Response_V0_TopicName_FlatIsr_RoundTrips()
    {
        var original = new AlterPartitionResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            Topics =
            [
                new AlterPartitionResponse.TopicData
                {
                    TopicName = "orders",
                    Partitions =
                    [
                        new AlterPartitionResponse.PartitionData
                        {
                            PartitionIndex = 0,
                            ErrorCode = ErrorCode.None,
                            LeaderId = 1,
                            LeaderEpoch = 5,
                            Isr = [1, 2, 3],
                            LeaderRecoveryState = 0,
                            PartitionEpoch = 42,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterPartitionResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Equal("orders", parsed.Topics[0].TopicName);
        Assert.Equal(new[] { 1, 2, 3 }, parsed.Topics[0].Partitions[0].Isr);
        Assert.Null(parsed.Topics[0].Partitions[0].IsrWithEpochs);
    }

    [Fact]
    public void Response_V2_TopicId_RoundTrips()
    {
        var original = new AlterPartitionResponse
        {
            ApiVersion = 2,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            Topics =
            [
                new AlterPartitionResponse.TopicData
                {
                    TopicId = TopicIdA,
                    Partitions =
                    [
                        new AlterPartitionResponse.PartitionData
                        {
                            PartitionIndex = 0,
                            ErrorCode = ErrorCode.None,
                            LeaderId = 1,
                            LeaderEpoch = 5,
                            Isr = [1, 2, 3],
                            LeaderRecoveryState = 0,
                            PartitionEpoch = 42,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterPartitionResponse.ReadFrom(reader, apiVersion: 2, correlationId: 1);

        Assert.Equal(TopicIdA, parsed.Topics[0].TopicId);
        Assert.Null(parsed.Topics[0].TopicName);
    }

    [Fact]
    public void Response_V3_IsrWithEpochs_RoundTrips()
    {
        var original = new AlterPartitionResponse
        {
            ApiVersion = 3,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            Topics =
            [
                new AlterPartitionResponse.TopicData
                {
                    TopicId = TopicIdA,
                    Partitions =
                    [
                        new AlterPartitionResponse.PartitionData
                        {
                            PartitionIndex = 0,
                            ErrorCode = ErrorCode.None,
                            LeaderId = 1,
                            LeaderEpoch = 6,
                            IsrWithEpochs =
                            [
                                new AlterPartitionResponse.BrokerState { BrokerId = 1, BrokerEpoch = 17 },
                                new AlterPartitionResponse.BrokerState { BrokerId = 2, BrokerEpoch = 23 },
                            ],
                            LeaderRecoveryState = 0,
                            PartitionEpoch = 43, // bumped after controller accepted the change
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterPartitionResponse.ReadFrom(reader, apiVersion: 3, correlationId: 1);

        var p = parsed.Topics[0].Partitions[0];
        Assert.Null(p.Isr);
        Assert.NotNull(p.IsrWithEpochs);
        Assert.Equal(2, p.IsrWithEpochs!.Count);
        Assert.Equal(23L, p.IsrWithEpochs[1].BrokerEpoch);
        Assert.Equal(43, p.PartitionEpoch);
    }

    [Fact]
    public void Response_V3_PerPartitionFencedError_RoundTrips()
    {
        // FENCED_LEADER_EPOCH-style mismatch: partition 1 succeeded,
        // partition 99 was rejected because the leader epoch was stale.
        // Surgewave's enum doesn't carry a dedicated value for that
        // semantic — use NotController as the closest match.
        var original = new AlterPartitionResponse
        {
            ApiVersion = 3,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            Topics =
            [
                new AlterPartitionResponse.TopicData
                {
                    TopicId = TopicIdA,
                    Partitions =
                    [
                        new AlterPartitionResponse.PartitionData
                        {
                            PartitionIndex = 1,
                            ErrorCode = ErrorCode.None,
                            LeaderId = 1, LeaderEpoch = 5,
                            IsrWithEpochs = [new AlterPartitionResponse.BrokerState { BrokerId = 1, BrokerEpoch = 17 }],
                            LeaderRecoveryState = 0, PartitionEpoch = 43,
                        },
                        new AlterPartitionResponse.PartitionData
                        {
                            PartitionIndex = 99,
                            ErrorCode = ErrorCode.NotController, // closest available to FENCED_LEADER_EPOCH
                            LeaderId = -1, LeaderEpoch = -1,
                            IsrWithEpochs = [], // empty on error
                            LeaderRecoveryState = 0, PartitionEpoch = -1,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterPartitionResponse.ReadFrom(reader, apiVersion: 3, correlationId: 1);

        Assert.Equal(2, parsed.Topics[0].Partitions.Count);
        Assert.Equal(ErrorCode.None, parsed.Topics[0].Partitions[0].ErrorCode);
        Assert.Equal(ErrorCode.NotController, parsed.Topics[0].Partitions[1].ErrorCode);
        Assert.Empty(parsed.Topics[0].Partitions[1].IsrWithEpochs!);
    }
}

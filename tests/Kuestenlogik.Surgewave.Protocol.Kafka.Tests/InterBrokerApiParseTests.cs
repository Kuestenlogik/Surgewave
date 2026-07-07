using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Gate 1 for #69: the broker parser must decode the controller-push inter-broker
/// ApiKeys LeaderAndIsr(4)/StopReplica(5)/UpdateMetadata(6). Before the fix the
/// parser switch had no cases for these, so a follower threw
/// "API key LeaderAndIsr is not supported" at KafkaProtocolHandler during PARSE
/// (before the dispatcher/handler was ever consulted) — the true first blocker
/// that made legacy follower replication impossible. These tests serialize the
/// exact request shapes ControllerClient sends and assert the parser returns the
/// typed request rather than throwing.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class InterBrokerApiParseTests
{
    private static readonly KafkaProtocolHandler Handler = new();

    [Fact]
    public void ParseRequest_LeaderAndIsr_V4_ReturnsTypedRequest()
    {
        var request = new LeaderAndIsrRequest
        {
            ApiKey = ApiKey.LeaderAndIsr,
            ApiVersion = 4, // v4+ is flexible — exactly what ControllerClient sends
            CorrelationId = 7,
            ClientId = "surgewave-controller-1",
            ControllerId = 1,
            ControllerEpoch = 1,
            BrokerEpoch = -1,
            Type = 0,
            TopicStates =
            [
                new LeaderAndIsrRequest.LeaderAndIsrTopicState
                {
                    TopicName = "orders",
                    TopicId = Guid.NewGuid(),
                    PartitionStates =
                    [
                        new LeaderAndIsrRequest.LeaderAndIsrPartitionState
                        {
                            PartitionIndex = 0,
                            ControllerEpoch = 1,
                            Leader = 2,
                            LeaderEpoch = 1,
                            Isr = [2],
                            PartitionEpoch = 1,
                            Replicas = [2, 3, 1],
                            AddingReplicas = [],
                            RemovingReplicas = [],
                            IsNew = false,
                        },
                    ],
                },
            ],
            LiveLeaders =
            [
                new LeaderAndIsrRequest.LeaderAndIsrLiveLeader { BrokerId = 1, Host = "127.0.0.1", Port = 9092 },
                new LeaderAndIsrRequest.LeaderAndIsrLiveLeader { BrokerId = 2, Host = "127.0.0.1", Port = 9093 },
            ],
        };

        var parsed = Handler.ParseRequest(request.Serialize());

        var lai = Assert.IsType<LeaderAndIsrRequest>(parsed);
        Assert.Equal(1, lai.ControllerId);
        Assert.Equal("orders", Assert.Single(lai.TopicStates).TopicName);
        var partition = Assert.Single(lai.TopicStates[0].PartitionStates);
        Assert.Equal(2, partition.Leader);
        Assert.Equal(new[] { 2, 3, 1 }, partition.Replicas);
        Assert.Equal(2, lai.LiveLeaders.Count);
    }

    [Fact]
    public void ParseRequest_StopReplica_V3_ReturnsTypedRequest()
    {
        var request = new StopReplicaRequest
        {
            ApiKey = ApiKey.StopReplica,
            ApiVersion = 3, // v2+ is flexible — what ControllerClient sends
            CorrelationId = 8,
            ClientId = "surgewave-controller-1",
            ControllerId = 1,
            ControllerEpoch = 1,
            BrokerEpoch = -1,
            TopicStates =
            [
                new StopReplicaRequest.StopReplicaTopicState
                {
                    TopicName = "orders",
                    PartitionStates =
                    [
                        new StopReplicaRequest.StopReplicaPartitionState
                        {
                            PartitionIndex = 0,
                            LeaderEpoch = 1,
                            DeletePartition = false,
                        },
                    ],
                },
            ],
        };

        var parsed = Handler.ParseRequest(request.Serialize());

        var stop = Assert.IsType<StopReplicaRequest>(parsed);
        Assert.Equal(1, stop.ControllerId);
        Assert.Equal("orders", Assert.Single(stop.TopicStates!).TopicName);
    }

    [Fact]
    public void ParseRequest_UpdateMetadata_V6_ReturnsTypedRequest()
    {
        var request = new UpdateMetadataRequest
        {
            ApiKey = ApiKey.UpdateMetadata,
            ApiVersion = 6, // v6+ is flexible — what ControllerClient sends
            CorrelationId = 9,
            ClientId = "surgewave-controller-1",
            ControllerId = 1,
            ControllerEpoch = 1,
            BrokerEpoch = -1,
            TopicStates =
            [
                new UpdateMetadataRequest.UpdateMetadataTopicState
                {
                    TopicName = "orders",
                    TopicId = Guid.NewGuid(),
                    PartitionStates =
                    [
                        new UpdateMetadataRequest.UpdateMetadataPartitionState
                        {
                            PartitionIndex = 0,
                            ControllerEpoch = 1,
                            Leader = 2,
                            LeaderEpoch = 1,
                            Isr = [2, 3, 1],
                            ZkVersion = 0,
                            Replicas = [2, 3, 1],
                            OfflineReplicas = [],
                        },
                    ],
                },
            ],
            LiveBrokers =
            [
                new UpdateMetadataRequest.UpdateMetadataBroker { Id = 1, Endpoints = [] },
                new UpdateMetadataRequest.UpdateMetadataBroker { Id = 2, Endpoints = [] },
            ],
        };

        var parsed = Handler.ParseRequest(request.Serialize());

        var update = Assert.IsType<UpdateMetadataRequest>(parsed);
        Assert.Equal(1, update.ControllerId);
        var partition = Assert.Single(Assert.Single(update.TopicStates!).PartitionStates);
        Assert.Equal(new[] { 2, 3, 1 }, partition.Isr);
        Assert.Equal(2, update.LiveBrokers.Count);
    }

    [Fact]
    public void ParseRequest_AlterPartition_V3_ReturnsTypedRequest()
    {
        // Reverse ISR propagation (#69 Phase 2): a leader sends AlterPartition v3
        // to the controller. AlterPartition is flexible at v0+, so the header has
        // trailing tagged fields AND ClientId must be a regular (non-compact)
        // string — the two header fixes this test locks in. The 9 existing
        // round-trip tests bypass ReadRequestHeader and cannot catch them.
        var request = new AlterPartitionRequest
        {
            ApiKey = ApiKey.AlterPartition,
            ApiVersion = 3, // v3: NewIsrWithEpochs + TopicId — exactly what ControllerClient sends
            CorrelationId = 11,
            ClientId = "surgewave-leader-2",
            BrokerId = 2,
            BrokerEpoch = -1,
            Topics =
            [
                new AlterPartitionRequest.TopicData
                {
                    TopicId = Guid.NewGuid(),
                    Partitions =
                    [
                        new AlterPartitionRequest.PartitionData
                        {
                            PartitionIndex = 1,
                            LeaderEpoch = 4,
                            PartitionEpoch = 4,
                            LeaderRecoveryState = 0,
                            NewIsrWithEpochs =
                            [
                                new AlterPartitionRequest.BrokerState { BrokerId = 2, BrokerEpoch = -1 },
                                new AlterPartitionRequest.BrokerState { BrokerId = 3, BrokerEpoch = -1 },
                                new AlterPartitionRequest.BrokerState { BrokerId = 1, BrokerEpoch = -1 },
                            ],
                        },
                    ],
                },
            ],
        };

        var parsed = Handler.ParseRequest(request.Serialize());

        var alter = Assert.IsType<AlterPartitionRequest>(parsed);
        Assert.Equal(2, alter.BrokerId);
        var partition = Assert.Single(Assert.Single(alter.Topics).Partitions);
        Assert.Equal(1, partition.PartitionIndex);
        Assert.Equal(4, partition.LeaderEpoch);
        Assert.NotNull(partition.NewIsrWithEpochs);
        Assert.Equal(new[] { 2, 3, 1 }, partition.NewIsrWithEpochs!.Select(b => b.BrokerId).ToArray());
    }
}

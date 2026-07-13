using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc5 — the SHARED controller-epoch fence, exercised through the Kafka-wire
/// <see cref="InterBrokerApiHandler"/>. The regression this locks in: the pre-Inc5 handler fenced on
/// a PRIVATE _currentControllerEpoch field, so an epoch the NATIVE wire had already advanced on the
/// shared <see cref="ClusterState"/> was invisible to it, and a stale Kafka-wire push could regress
/// partition metadata during a mixed-wire rolling upgrade. Both wires now fence through
/// <see cref="ClusterState.TryAdvanceControllerEpoch"/>.
/// </summary>
[Trait("Category", TestCategories.Integration)]
public sealed class CrossWireFenceTests
{
    private static (InterBrokerApiHandler Handler, ClusterState State) NewHandler(int brokerId = 0)
    {
        var config = new BrokerConfig { BrokerId = brokerId };
        var state = new ClusterState();
        var logs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logs, new ClusteringConfig { BrokerId = brokerId },
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        var handler = new InterBrokerApiHandler(
            config, state, replicas, logs, NullLogger<InterBrokerApiHandler>.Instance);
        return (handler, state);
    }

    private static readonly RequestContext Ctx =
        new() { ConnectionState = new ConnectionState("fence-test"), ClientId = "controller" };

    [Fact]
    public async Task KafkaWireUpdateMetadata_StaleAgainstNativelyAdvancedEpoch_IsRejected()
    {
        var (handler, state) = NewHandler();

        // Simulate a native-wire advance: the shared ClusterState epoch is now 6, controller 9.
        Assert.True(state.TryAdvanceControllerEpoch(controllerId: 9, controllerEpoch: 6));

        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        var request = new UpdateMetadataRequest
        {
            ApiKey = ApiKey.UpdateMetadata,
            ApiVersion = 6,
            CorrelationId = 1,
            ClientId = "controller",
            ControllerId = 4,
            ControllerEpoch = 5, // stale: a demoted controller, older than the native-advanced 6
            BrokerEpoch = -1,
            TopicStates =
            [
                new UpdateMetadataRequest.UpdateMetadataTopicState
                {
                    TopicName = tp.Topic,
                    PartitionStates =
                    [
                        new UpdateMetadataRequest.UpdateMetadataPartitionState
                        {
                            PartitionIndex = tp.Partition, ControllerEpoch = 5, Leader = 4,
                            LeaderEpoch = 3, Isr = [4], Replicas = [4], OfflineReplicas = [], ZkVersion = 0,
                        },
                    ],
                },
            ],
            LiveBrokers = [],
        };

        var response = (UpdateMetadataResponse)await handler.HandleAsync(request, Ctx, CancellationToken.None);

        Assert.Equal(ErrorCode.StaleControllerEpoch, response.ErrorCode);
        // Nothing applied and the shared epoch/id did NOT regress to the stale controller's view.
        Assert.Null(state.GetPartitionState(tp));
        Assert.Equal(6, state.ControllerEpoch);
        Assert.Equal(9, state.ControllerId);
    }

    [Fact]
    public async Task KafkaWireUpdateMetadata_FresherEpoch_AdvancesSharedState()
    {
        var (handler, state) = NewHandler();
        state.TryAdvanceControllerEpoch(controllerId: 1, controllerEpoch: 2);

        var request = new UpdateMetadataRequest
        {
            ApiKey = ApiKey.UpdateMetadata,
            ApiVersion = 6,
            CorrelationId = 2,
            ClientId = "controller",
            ControllerId = 5,
            ControllerEpoch = 7, // fresher — must advance the shared state the native wire also reads
            BrokerEpoch = -1,
            TopicStates = [],
            LiveBrokers = [],
        };

        var response = (UpdateMetadataResponse)await handler.HandleAsync(request, Ctx, CancellationToken.None);

        Assert.Equal(ErrorCode.None, response.ErrorCode);
        Assert.Equal(7, state.ControllerEpoch);
        Assert.Equal(5, state.ControllerId);
    }

    // ── #60 Inc6a: per-partition leader-epoch guard is symmetric on the Kafka wire ─────────────

    [Fact]
    public async Task KafkaWireStopReplica_StaleLeaderEpoch_DoesNotDeleteReassignedPartition()
    {
        var (handler, state) = NewHandler();
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        // The partition was re-assigned at leader epoch 6.
        state.TryApplyControllerPartitionState(tp, leaderId: 0, leaderEpoch: 6, replicas: [0], isr: [0]);

        // A delayed StopReplica(delete) at the OLDER epoch 5 must be refused, not delete the data.
        var request = new StopReplicaRequest
        {
            ApiKey = ApiKey.StopReplica,
            ApiVersion = 3,
            CorrelationId = 1,
            ClientId = "controller",
            ControllerId = 1,
            ControllerEpoch = 0,
            BrokerEpoch = -1,
            DeletePartitions = true,
            TopicStates =
            [
                new StopReplicaRequest.StopReplicaTopicState
                {
                    TopicName = tp.Topic,
                    PartitionStates = [new StopReplicaRequest.StopReplicaPartitionState { PartitionIndex = 0, LeaderEpoch = 5, DeletePartition = true }],
                },
            ],
        };

        var response = (StopReplicaResponse)await handler.HandleAsync(request, Ctx, CancellationToken.None);

        Assert.Equal(ErrorCode.None, response.ErrorCode);
        // The re-assigned partition state must survive — the stale stop was skipped.
        Assert.NotNull(state.GetPartitionState(tp));
        Assert.Equal(6, state.GetPartitionState(tp)!.LeaderEpoch);
    }

    [Fact]
    public async Task KafkaWireLeaderAndIsr_StaleLeaderEpoch_DoesNotRegressPartition()
    {
        var (handler, state) = NewHandler();
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        state.TryApplyControllerPartitionState(tp, leaderId: 4, leaderEpoch: 9, replicas: [4], isr: [4]);

        var request = new LeaderAndIsrRequest
        {
            ApiKey = ApiKey.LeaderAndIsr,
            ApiVersion = 4,
            CorrelationId = 1,
            ClientId = "controller",
            ControllerId = 1,
            ControllerEpoch = 0,
            BrokerEpoch = -1,
            Type = 0,
            TopicStates =
            [
                new LeaderAndIsrRequest.LeaderAndIsrTopicState
                {
                    TopicName = tp.Topic,
                    TopicId = Guid.NewGuid(),
                    PartitionStates =
                    [
                        new LeaderAndIsrRequest.LeaderAndIsrPartitionState
                        {
                            PartitionIndex = 0, ControllerEpoch = 0, Leader = 1, LeaderEpoch = 2,
                            Isr = [1], PartitionEpoch = 2, Replicas = [1], AddingReplicas = [], RemovingReplicas = [], IsNew = false,
                        },
                    ],
                },
            ],
            LiveLeaders = [],
        };

        var response = (LeaderAndIsrResponse)await handler.HandleAsync(request, Ctx, CancellationToken.None);

        Assert.Equal(ErrorCode.None, response.ErrorCode);
        // Stale leader epoch 2 < stored 9 — the fresh leader 4 must not be regressed.
        Assert.Equal(4, state.GetPartitionState(tp)!.LeaderBrokerId);
        Assert.Equal(9, state.GetPartitionState(tp)!.LeaderEpoch);
    }
}

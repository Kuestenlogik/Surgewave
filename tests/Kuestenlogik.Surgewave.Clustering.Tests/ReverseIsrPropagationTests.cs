using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Controller-side reverse-ISR propagation (#69 Phase 2): a partition leader
/// reports its in-sync set via AlterPartition and the controller applies it to
/// its authoritative <see cref="ClusterState"/> — the store that backs the
/// Kafka Metadata served to clients. Verifies the apply, the leader-epoch fence,
/// and the not-controller guard of
/// <see cref="ClusterController.ApplyIsrUpdateAsync"/>.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ReverseIsrPropagationTests
{
    private static ClusteringConfig Config(int brokerId) => new()
    {
        BrokerId = brokerId,
        Host = "localhost",
        Port = 9092 + brokerId,
        RebalanceCheckIntervalSeconds = 5,
    };

    private static (ClusterController Controller, ClusterState State, LogManager Logs) NewController(int brokerId, params int[] brokers)
    {
        var config = Config(brokerId);
        var state = new ClusterState();
        foreach (var id in brokers)
            state.AddBroker(new BrokerNode { BrokerId = id, Host = "localhost", Port = 9092 + id });

        var logs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicaManager = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logs, config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        var controller = new ClusterController(
            NullLogger<ClusterController>.Instance, state, replicaManager, config);
        return (controller, state, logs);
    }

    [Fact]
    public async Task ApplyIsrUpdate_AsController_GrowsIsrToReportedSet()
    {
        var (controller, state, logs) = NewController(brokerId: 0, brokers: [0, 1, 2]);
        await controller.StartAsync(CancellationToken.None); // lowest id -> this broker is controller
        Assert.True(controller.IsController);

        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        state.AssignReplicas(tp, [0, 1, 2], 1); // initial ISR = {0} (leader only)
        var epoch = state.GetPartitionState(tp)!.LeaderEpoch;

        var updated = await controller.ApplyIsrUpdateAsync(tp, leaderId: 0, leaderEpoch: epoch, newIsr: [0, 1, 2]);

        Assert.NotNull(updated);
        Assert.Equal(new[] { 0, 1, 2 }, updated!.Isr.OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 0, 1, 2 }, state.GetPartitionState(tp)!.Isr.OrderBy(x => x).ToArray());

        await controller.DisposeAsync();
        logs.Dispose();
    }

    [Fact]
    public async Task ApplyIsrUpdate_StaleLeaderEpoch_RejectedWithoutChange()
    {
        var (controller, state, logs) = NewController(brokerId: 0, brokers: [0, 1, 2]);
        await controller.StartAsync(CancellationToken.None);

        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        state.AssignReplicas(tp, [0, 1, 2], 1);
        state.GetPartitionState(tp)!.LeaderEpoch = 5; // controller has advanced past the reporter

        var updated = await controller.ApplyIsrUpdateAsync(tp, leaderId: 0, leaderEpoch: 2, newIsr: [0, 1, 2]);

        Assert.NotNull(updated); // current state is returned…
        Assert.Equal(new[] { 0 }, state.GetPartitionState(tp)!.Isr.ToArray()); // …but the ISR is unchanged

        await controller.DisposeAsync();
        logs.Dispose();
    }

    [Fact]
    public async Task ApplyIsrUpdate_NotController_ReturnsNull()
    {
        // Broker 5 in a cluster whose lowest id (0) becomes the controller.
        var (controller, state, logs) = NewController(brokerId: 5, brokers: [0, 5]);
        await controller.StartAsync(CancellationToken.None);
        Assert.False(controller.IsController);

        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        state.AssignReplicas(tp, [5, 0], 1);

        var updated = await controller.ApplyIsrUpdateAsync(tp, leaderId: 5, leaderEpoch: 0, newIsr: [5, 0]);

        Assert.Null(updated);

        await controller.DisposeAsync();
        logs.Dispose();
    }
}

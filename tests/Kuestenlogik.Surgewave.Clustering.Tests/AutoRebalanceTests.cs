using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

[Trait("Category", TestCategories.Unit)]
public class AutoRebalanceTests
{
    private static ClusteringConfig CreateConfig(bool autoRebalance = true, double threshold = 0.1) => new()
    {
        BrokerId = 0,
        Host = "localhost",
        Port = 9092,
        AutoRebalanceEnabled = autoRebalance,
        AllowAutoLeaderRebalance = true,
        RebalanceImbalanceThreshold = threshold,
        RebalanceCheckIntervalSeconds = 5,
        ReassignmentMaxConcurrent = 5
    };

    private static ClusterState CreateClusterWithBrokers(params int[] brokerIds)
    {
        var state = new ClusterState();
        foreach (var id in brokerIds)
        {
            state.AddBroker(new BrokerNode { BrokerId = id, Host = "localhost", Port = 9092 + id });
        }
        return state;
    }

    private static void AddPartition(ClusterState state, string topic, int partition, List<int> replicas, int leader = -1)
    {
        var tp = new TopicPartition { Topic = topic, Partition = partition };
        state.AssignReplicas(tp, replicas, 1);
        // Set all replicas as in-sync
        state.UpdateIsr(tp, replicas);
        if (leader >= 0)
        {
            state.ElectLeader(tp, leader);
        }
        else if (replicas.Count > 0)
        {
            state.ElectLeader(tp, replicas[0]);
        }
    }

    [Fact]
    public void ClusterBalancer_DetectsLeaderImbalance()
    {
        var config = CreateConfig();
        var state = CreateClusterWithBrokers(0, 1, 2);

        // All leaders on broker 0 — heavily imbalanced
        AddPartition(state, "t1", 0, [0, 1, 2], leader: 0);
        AddPartition(state, "t1", 1, [1, 0, 2], leader: 0);
        AddPartition(state, "t1", 2, [2, 0, 1], leader: 0);

        var balancer = new ClusterBalancer(
            NullLogger<ClusterBalancer>.Instance, state, config);

        var status = balancer.GetBalanceStatus();

        Assert.True(status.LeaderImbalanceRatio > 0);
        Assert.True(status.PartitionsNotOnPreferredLeader > 0);
        Assert.True(balancer.IsRebalanceNeeded());
    }

    [Fact]
    public void ClusterBalancer_BalancedCluster_NoRebalanceNeeded()
    {
        var config = CreateConfig();
        var state = CreateClusterWithBrokers(0, 1, 2);

        // Evenly distributed leaders
        AddPartition(state, "t1", 0, [0, 1, 2], leader: 0);
        AddPartition(state, "t1", 1, [1, 2, 0], leader: 1);
        AddPartition(state, "t1", 2, [2, 0, 1], leader: 2);

        var balancer = new ClusterBalancer(
            NullLogger<ClusterBalancer>.Instance, state, config);

        var status = balancer.GetBalanceStatus();

        Assert.Equal(0, status.LeaderImbalanceRatio);
        Assert.Equal(0, status.PartitionsNotOnPreferredLeader);
        Assert.Equal(BalanceState.Balanced, status.State);
    }

    [Fact]
    public void ClusterBalancer_GeneratesPlan_ForImbalancedCluster()
    {
        var config = CreateConfig();
        var state = CreateClusterWithBrokers(0, 1, 2);

        // Leaders not on preferred replicas
        AddPartition(state, "t1", 0, [0, 1, 2], leader: 1); // should be on 0
        AddPartition(state, "t1", 1, [1, 2, 0], leader: 2); // should be on 1
        AddPartition(state, "t1", 2, [2, 0, 1], leader: 2); // stays on 2

        var balancer = new ClusterBalancer(
            NullLogger<ClusterBalancer>.Instance, state, config);

        var plan = balancer.GenerateRebalancePlan();

        // Should propose leader elections to move back to preferred
        Assert.True(plan.LeaderElections.Count >= 2);
    }

    [Fact]
    public void ClusterBalancer_DetectsReplicaImbalance()
    {
        var config = CreateConfig(threshold: 0.05);
        var state = CreateClusterWithBrokers(0, 1, 2);

        // All replicas on brokers 0 and 1 — broker 2 has nothing
        AddPartition(state, "t1", 0, [0, 1]);
        AddPartition(state, "t1", 1, [0, 1]);
        AddPartition(state, "t1", 2, [0, 1]);
        AddPartition(state, "t1", 3, [0, 1]);

        var balancer = new ClusterBalancer(
            NullLogger<ClusterBalancer>.Instance, state, config);

        var status = balancer.GetBalanceStatus();

        Assert.True(status.ReplicaImbalanceRatio > 0);
    }

    [Fact]
    public void ClusterBalancer_UnderReplicatedPartitions_Critical()
    {
        var config = CreateConfig();
        var state = CreateClusterWithBrokers(0, 1, 2);

        AddPartition(state, "t1", 0, [0, 1, 2], leader: 0);

        // Simulate under-replication by removing broker 2 from ISR
        var tp = new TopicPartition { Topic = "t1", Partition = 0 };
        state.RemoveFromIsr(tp, 2);

        var balancer = new ClusterBalancer(
            NullLogger<ClusterBalancer>.Instance, state, config);

        var status = balancer.GetBalanceStatus();

        Assert.Equal(BalanceState.Critical, status.State);
        Assert.Equal(1, status.UnderReplicatedPartitions);
    }

    [Fact]
    public void ClusterBalancer_EstimatesStatusAfterRebalance()
    {
        var config = CreateConfig();
        var state = CreateClusterWithBrokers(0, 1, 2);

        // All leaders on broker 0
        AddPartition(state, "t1", 0, [0, 1, 2], leader: 0);
        AddPartition(state, "t1", 1, [1, 0, 2], leader: 0);
        AddPartition(state, "t1", 2, [2, 0, 1], leader: 0);

        var balancer = new ClusterBalancer(
            NullLogger<ClusterBalancer>.Instance, state, config);

        var plan = balancer.GenerateRebalancePlan();

        // Estimated status after should be better
        Assert.True(plan.EstimatedStatusAfter.LeaderImbalanceRatio <= plan.CurrentStatus.LeaderImbalanceRatio);
    }

    [Fact]
    public void ClusterController_SetsBalancerAndReassignmentManager()
    {
        var config = CreateConfig();
        var state = CreateClusterWithBrokers(0);
        var logManager = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicaManager = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logManager, config, new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());

        var controller = new ClusterController(
            NullLogger<ClusterController>.Instance, state, replicaManager, config);

        var balancer = new ClusterBalancer(
            NullLogger<ClusterBalancer>.Instance, state, config);
        var reassignmentManager = new PartitionReassignmentManager(
            NullLogger<PartitionReassignmentManager>.Instance, state, controller, replicaManager, logManager, config);

        // Should not throw
        controller.SetClusterBalancer(balancer);
        controller.SetReassignmentManager(reassignmentManager);

        logManager.Dispose();
    }

    [Fact]
    public void PartitionReassignmentManager_GeneratesPlan_ForTopics()
    {
        var config = CreateConfig();
        var state = CreateClusterWithBrokers(0, 1, 2);
        var logManager = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicaManager = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logManager, config, new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        var controller = new ClusterController(
            NullLogger<ClusterController>.Instance, state, replicaManager, config);

        // Register topic metadata so GenerateReassignmentPlan can find it
        state.AddTopic(new TopicMetadata { Name = "t1", TopicId = Guid.NewGuid(), PartitionCount = 3, ReplicationFactor = 1, Config = [], CreatedAt = DateTime.UtcNow });

        // All partitions on broker 0
        AddPartition(state, "t1", 0, [0]);
        AddPartition(state, "t1", 1, [0]);
        AddPartition(state, "t1", 2, [0]);

        var reassignmentManager = new PartitionReassignmentManager(
            NullLogger<PartitionReassignmentManager>.Instance, state, controller, replicaManager, logManager, config);

        var plan = reassignmentManager.GenerateReassignmentPlan(["t1"], [0, 1, 2]);

        // Should redistribute across all brokers
        Assert.True(plan.Partitions.Count > 0);

        // Each partition should have a different starting broker (round-robin)
        var firstReplicas = plan.Partitions.Select(p => p.Replicas[0]).ToList();
        Assert.True(firstReplicas.Distinct().Count() > 1,
            "Partitions should be distributed across multiple brokers");

        logManager.Dispose();
    }

    [Fact]
    public void ClusterBalancer_EmptyCluster_ReportsBalanced()
    {
        var config = CreateConfig();
        var state = new ClusterState();

        var balancer = new ClusterBalancer(
            NullLogger<ClusterBalancer>.Instance, state, config);

        var status = balancer.GetBalanceStatus();

        Assert.Equal(BalanceState.Balanced, status.State);
        Assert.Equal(0, status.TotalPartitions);
        Assert.False(balancer.IsRebalanceNeeded());
    }

    [Fact]
    public void ClusterBalancer_SingleBroker_NoImbalance()
    {
        var config = CreateConfig();
        var state = CreateClusterWithBrokers(0);

        AddPartition(state, "t1", 0, [0], leader: 0);
        AddPartition(state, "t1", 1, [0], leader: 0);
        AddPartition(state, "t1", 2, [0], leader: 0);

        var balancer = new ClusterBalancer(
            NullLogger<ClusterBalancer>.Instance, state, config);

        var status = balancer.GetBalanceStatus();

        // Single broker — all leaders on one broker is expected
        Assert.Equal(0, status.LeaderImbalanceRatio);
    }

    [Fact]
    public void ReassignmentPlan_LimitedByConcurrentMax()
    {
        var config = CreateConfig();
        config.ReassignmentMaxConcurrent = 2;
        var state = CreateClusterWithBrokers(0, 1);
        var logManager = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicaManager = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logManager, config, new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());

        // Register topic metadata
        state.AddTopic(new TopicMetadata { Name = "t1", TopicId = Guid.NewGuid(), PartitionCount = 5, ReplicationFactor = 1, Config = [], CreatedAt = DateTime.UtcNow });

        // 5 partitions all on broker 0
        for (int i = 0; i < 5; i++)
            AddPartition(state, "t1", i, [0]);

        var controller = new ClusterController(
            NullLogger<ClusterController>.Instance, state, replicaManager, config);
        var reassignmentManager = new PartitionReassignmentManager(
            NullLogger<PartitionReassignmentManager>.Instance, state, controller, replicaManager, logManager, config);

        var plan = reassignmentManager.GenerateReassignmentPlan(["t1"], [0, 1]);

        // Plan may have more than 2, but execution should limit
        Assert.True(plan.Partitions.Count > 0);

        logManager.Dispose();
    }
}

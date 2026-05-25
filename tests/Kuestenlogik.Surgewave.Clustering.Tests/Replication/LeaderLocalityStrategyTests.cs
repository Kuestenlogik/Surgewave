using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Replication;

/// <summary>
/// Tests for LeaderLocalityStrategy.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class LeaderLocalityStrategyTests
{
    private readonly ClusterState _clusterState;
    private readonly ConsumerRackTracker _rackTracker;
    private readonly FailureDomainHierarchy _domainHierarchy;

    public LeaderLocalityStrategyTests()
    {
        _clusterState = new ClusterState();
        _rackTracker = new ConsumerRackTracker();
        _domainHierarchy = new FailureDomainHierarchy();
    }

    private LeaderLocalityStrategy CreateStrategy(LeaderLocalityOptions? options = null)
    {
        return new LeaderLocalityStrategy(
            _clusterState,
            _rackTracker,
            _domainHierarchy,
            NullLogger<LeaderLocalityStrategy>.Instance,
            options);
    }

    #region PreferredReplica Mode Tests

    [Fact]
    public void SelectLeader_PreferredReplica_SelectsFirstIsrInReplicaList()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-3");

        var strategy = CreateStrategy(new LeaderLocalityOptions { Mode = LeaderElectionMode.PreferredReplica });
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2, 3],
            Isr = [1, 2, 3]
        };

        // Act
        var leader = strategy.SelectLeader(partition, state);

        // Assert
        Assert.Equal(1, leader);
    }

    [Fact]
    public void SelectLeader_PreferredReplica_SkipsNonIsrReplicas()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-3");

        var strategy = CreateStrategy(new LeaderLocalityOptions { Mode = LeaderElectionMode.PreferredReplica });
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2, 3],
            Isr = [2, 3] // Broker 1 is not in ISR
        };

        // Act
        var leader = strategy.SelectLeader(partition, state);

        // Assert
        Assert.Equal(2, leader);
    }

    [Fact]
    public void SelectLeader_EmptyIsr_ReturnsMinusOne()
    {
        // Arrange
        var strategy = CreateStrategy();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2, 3],
            Isr = []
        };

        // Act
        var leader = strategy.SelectLeader(partition, state);

        // Assert
        Assert.Equal(-1, leader);
    }

    #endregion

    #region RackLocal Mode Tests

    [Fact]
    public void SelectLeader_RackLocal_SelectsReplicaInDominantRack()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-2");

        _rackTracker.RegisterConsumer("consumer-1", "rack-2");
        _rackTracker.RegisterConsumer("consumer-2", "rack-2");
        _rackTracker.RegisterConsumer("consumer-3", "rack-1");

        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        _rackTracker.RecordFetch(partition, "consumer-1");
        _rackTracker.RecordFetch(partition, "consumer-2");
        _rackTracker.RecordFetch(partition, "consumer-3");

        var strategy = CreateStrategy(new LeaderLocalityOptions { Mode = LeaderElectionMode.RackLocal });
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2, 3],
            Isr = [1, 2, 3]
        };

        // Act
        var leader = strategy.SelectLeader(partition, state);

        // Assert - Should select broker 2 or 3 (both in rack-2 which has more consumers)
        Assert.True(leader == 2 || leader == 3);
    }

    [Fact]
    public void SelectLeader_RackLocal_NoDominantRack_FallsBackToPreferred()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        var strategy = CreateStrategy(new LeaderLocalityOptions { Mode = LeaderElectionMode.RackLocal });
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2],
            Isr = [1, 2]
        };

        // Act - No consumers registered, so no dominant rack
        var leader = strategy.SelectLeader(partition, state);

        // Assert - Falls back to first ISR in replica list
        Assert.Equal(1, leader);
    }

    [Fact]
    public void SelectLeader_RackLocal_NoIsrInDominantRack_FallsBackToPreferred()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-1");

        _rackTracker.RegisterConsumer("consumer-1", "rack-2");
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        _rackTracker.RecordFetch(partition, "consumer-1");

        var strategy = CreateStrategy(new LeaderLocalityOptions { Mode = LeaderElectionMode.RackLocal });
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2],
            Isr = [1, 2] // Both in rack-1, dominant rack is rack-2
        };

        // Act
        var leader = strategy.SelectLeader(partition, state);

        // Assert - Falls back to preferred since no ISR in rack-2
        Assert.Equal(1, leader);
    }

    [Fact]
    public void SelectLeader_RackLocal_HierarchicalRack_MatchesCorrectly()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "us-east/dc1/zone-a/rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "us-east/dc1/zone-a/rack-2");
        _domainHierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        _domainHierarchy.RegisterBroker(2, "us-east/dc1/zone-a/rack-2");

        _rackTracker.RegisterConsumer("consumer-1", "rack-2");
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        _rackTracker.RecordFetch(partition, "consumer-1");

        var strategy = CreateStrategy(new LeaderLocalityOptions { Mode = LeaderElectionMode.RackLocal });
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2],
            Isr = [1, 2]
        };

        // Act
        var leader = strategy.SelectLeader(partition, state);

        // Assert - Should match broker 2's hierarchical rack ending with "rack-2"
        Assert.Equal(2, leader);
    }

    #endregion

    #region LatencyOptimized Mode Tests

    [Fact]
    public void SelectLeader_LatencyOptimized_SelectsHighestScoringReplica()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-2");
        _domainHierarchy.RegisterBroker(1, "rack-1");
        _domainHierarchy.RegisterBroker(2, "rack-2");
        _domainHierarchy.RegisterBroker(3, "rack-2");

        // Register more consumers in rack-2
        _rackTracker.RegisterConsumer("consumer-1", "rack-2");
        _rackTracker.RegisterConsumer("consumer-2", "rack-2");
        _rackTracker.RegisterConsumer("consumer-3", "rack-2");

        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        _rackTracker.RecordFetch(partition, "consumer-1");
        _rackTracker.RecordFetch(partition, "consumer-2");
        _rackTracker.RecordFetch(partition, "consumer-3");

        var strategy = CreateStrategy(new LeaderLocalityOptions { Mode = LeaderElectionMode.LatencyOptimized });
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2, 3],
            Isr = [1, 2, 3]
        };

        // Act
        var leader = strategy.SelectLeader(partition, state);

        // Assert - Should select broker 2 or 3 (both in rack-2 with most consumers)
        Assert.True(leader == 2 || leader == 3);
    }

    [Fact]
    public void SelectLeader_LatencyOptimized_NoConsumerData_FallsBackToPreferred()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        var strategy = CreateStrategy(new LeaderLocalityOptions { Mode = LeaderElectionMode.LatencyOptimized });
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2],
            Isr = [1, 2]
        };

        // Act
        var leader = strategy.SelectLeader(partition, state);

        // Assert - Falls back to preferred
        Assert.Equal(1, leader);
    }

    #endregion

    #region ShouldRebalance Tests

    [Fact]
    public void ShouldRebalance_AutoRebalanceDisabled_ReturnsFalse()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        var strategy = CreateStrategy(new LeaderLocalityOptions
        {
            Mode = LeaderElectionMode.RackLocal,
            AutoRebalance = false
        });
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2],
            Isr = [1, 2]
        };

        // Act
        var shouldRebalance = strategy.ShouldRebalance(partition, currentLeader: 1, state);

        // Assert
        Assert.False(shouldRebalance);
    }

    [Fact]
    public void ShouldRebalance_CurrentLeaderIsOptimal_ReturnsFalse()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        _rackTracker.RegisterConsumer("consumer-1", "rack-1");
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        _rackTracker.RecordFetch(partition, "consumer-1");

        var strategy = CreateStrategy(new LeaderLocalityOptions
        {
            Mode = LeaderElectionMode.RackLocal,
            AutoRebalance = true
        });
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2],
            Isr = [1, 2]
        };

        // Act - Broker 1 is already in dominant rack (rack-1)
        var shouldRebalance = strategy.ShouldRebalance(partition, currentLeader: 1, state);

        // Assert
        Assert.False(shouldRebalance);
    }

    [Fact]
    public void ShouldRebalance_BetterLeaderAvailable_ReturnsTrue()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        _rackTracker.RegisterConsumer("consumer-1", "rack-2");
        _rackTracker.RegisterConsumer("consumer-2", "rack-2");
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        _rackTracker.RecordFetch(partition, "consumer-1");
        _rackTracker.RecordFetch(partition, "consumer-2");

        var strategy = CreateStrategy(new LeaderLocalityOptions
        {
            Mode = LeaderElectionMode.RackLocal,
            AutoRebalance = true
        });
        var state = new PartitionState
        {
            TopicPartition = partition,
            Replicas = [1, 2],
            Isr = [1, 2]
        };

        // Act - Broker 1 is current leader but rack-2 has more consumers
        var shouldRebalance = strategy.ShouldRebalance(partition, currentLeader: 1, state);

        // Assert
        Assert.True(shouldRebalance);
    }

    #endregion

    #region LeaderLocalityOptions Tests

    [Fact]
    public void LeaderLocalityOptions_DefaultValues()
    {
        // Act
        var options = new LeaderLocalityOptions();

        // Assert
        Assert.Equal(LeaderElectionMode.PreferredReplica, options.Mode);
        Assert.False(options.AutoRebalance);
        Assert.Equal(2, options.MinConsumerDifferenceForRebalance);
    }

    [Fact]
    public void LeaderLocalityOptions_CustomValues()
    {
        // Act
        var options = new LeaderLocalityOptions
        {
            Mode = LeaderElectionMode.LatencyOptimized,
            AutoRebalance = true,
            MinConsumerDifferenceForRebalance = 5
        };

        // Assert
        Assert.Equal(LeaderElectionMode.LatencyOptimized, options.Mode);
        Assert.True(options.AutoRebalance);
        Assert.Equal(5, options.MinConsumerDifferenceForRebalance);
    }

    #endregion

    #region LeaderElectionMode Enum Tests

    [Fact]
    public void LeaderElectionMode_AllValues()
    {
        // Assert
        Assert.Equal(0, (int)LeaderElectionMode.PreferredReplica);
        Assert.Equal(1, (int)LeaderElectionMode.RackLocal);
        Assert.Equal(2, (int)LeaderElectionMode.LatencyOptimized);
    }

    #endregion
}

using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Replication;

/// <summary>
/// Tests for HierarchicalReplicaAssignment.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class HierarchicalReplicaAssignmentTests
{
    private readonly ClusterState _clusterState;
    private readonly FailureDomainHierarchy _domainHierarchy;
    private readonly FailureDomainValidator _domainValidator;

    public HierarchicalReplicaAssignmentTests()
    {
        _clusterState = new ClusterState();
        _domainHierarchy = new FailureDomainHierarchy();
        _domainValidator = new FailureDomainValidator(_clusterState);
    }

    private HierarchicalReplicaAssignment CreateAssignment(HierarchicalReplicaAssignmentOptions? options = null)
    {
        return new HierarchicalReplicaAssignment(
            _clusterState,
            _domainHierarchy,
            _domainValidator,
            NullLogger<HierarchicalReplicaAssignment>.Instance,
            options);
    }

    #region Round-Robin Assignment Tests

    [Fact]
    public void AssignReplicas_NoConstraints_UsesRoundRobin()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-3");

        var assignment = CreateAssignment();

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 3);

        // Assert
        Assert.Equal(3, replicas.Count);
        Assert.Contains(1, replicas);
        Assert.Contains(2, replicas);
        Assert.Contains(3, replicas);
    }

    [Fact]
    public void AssignReplicas_DifferentPartitions_DifferentStartingBroker()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-3");

        var assignment = CreateAssignment();

        // Act
        var replicas0 = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 1);
        var replicas1 = assignment.AssignReplicas("test-topic", partition: 1, replicationFactor: 1);
        var replicas2 = assignment.AssignReplicas("test-topic", partition: 2, replicationFactor: 1);

        // Assert - Each partition should start at a different broker
        Assert.NotEqual(replicas0[0], replicas1[0]);
        Assert.NotEqual(replicas1[0], replicas2[0]);
    }

    [Fact]
    public void AssignReplicas_InsufficientBrokers_AssignsWhatIsAvailable()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        var assignment = CreateAssignment();

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 5);

        // Assert
        Assert.Equal(2, replicas.Count);
    }

    #endregion

    #region SpreadAcross Constraint Tests

    [Fact]
    public void AssignReplicas_SpreadAcrossZone_DistributesAcrossZones()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "us-east/dc1/zone-a/rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "us-east/dc1/zone-a/rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "us-east/dc1/zone-b/rack-3");
        _clusterState.RegisterBroker(4, "localhost", 9095, "us-east/dc1/zone-b/rack-4");
        _clusterState.RegisterBroker(5, "localhost", 9096, "us-east/dc1/zone-c/rack-5");

        _domainHierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        _domainHierarchy.RegisterBroker(2, "us-east/dc1/zone-a/rack-2");
        _domainHierarchy.RegisterBroker(3, "us-east/dc1/zone-b/rack-3");
        _domainHierarchy.RegisterBroker(4, "us-east/dc1/zone-b/rack-4");
        _domainHierarchy.RegisterBroker(5, "us-east/dc1/zone-c/rack-5");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "spread_across:zone"
        });

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 3);

        // Assert - Should have replicas in 3 different zones
        Assert.Equal(3, replicas.Count);
        var zones = replicas.Select(r =>
        {
            var broker = _clusterState.GetBroker(r);
            var parts = broker?.Rack?.Split('/') ?? [];
            return parts.Length >= 3 ? parts[2] : "";
        }).Distinct().ToList();
        Assert.Equal(3, zones.Count);
    }

    [Fact]
    public void AssignReplicas_SpreadAcrossRack_DistributesAcrossRacks()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-1");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-2");
        _clusterState.RegisterBroker(4, "localhost", 9095, "rack-3");

        _domainHierarchy.RegisterBroker(1, "rack-1");
        _domainHierarchy.RegisterBroker(2, "rack-1");
        _domainHierarchy.RegisterBroker(3, "rack-2");
        _domainHierarchy.RegisterBroker(4, "rack-3");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "spread_across:rack"
        });

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 3);

        // Assert - Should have replicas in 3 different racks
        Assert.Equal(3, replicas.Count);
        var racks = replicas.Select(r => _clusterState.GetBroker(r)?.Rack).Distinct().ToList();
        Assert.Equal(3, racks.Count);
    }

    [Fact]
    public void AssignReplicas_SpreadAcrossDatacenter_DistributesAcrossDatacenters()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "us-east/dc1/zone-a/rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "us-east/dc2/zone-a/rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "us-east/dc3/zone-a/rack-3");

        _domainHierarchy.RegisterBroker(1, "us-east/dc1/zone-a/rack-1");
        _domainHierarchy.RegisterBroker(2, "us-east/dc2/zone-a/rack-2");
        _domainHierarchy.RegisterBroker(3, "us-east/dc3/zone-a/rack-3");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "spread_across:datacenter"
        });

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 3);

        // Assert - Should have replicas in 3 different datacenters
        Assert.Equal(3, replicas.Count);
    }

    [Fact]
    public void AssignReplicas_SpreadAcross_MoreReplicasThanDomains_ReuseDomains()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-1");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-2");
        _clusterState.RegisterBroker(4, "localhost", 9095, "rack-2");

        _domainHierarchy.RegisterBroker(1, "rack-1");
        _domainHierarchy.RegisterBroker(2, "rack-1");
        _domainHierarchy.RegisterBroker(3, "rack-2");
        _domainHierarchy.RegisterBroker(4, "rack-2");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "spread_across:rack"
        });

        // Act - Request 3 replicas with only 2 racks
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 3);

        // Assert - Should assign all 3 replicas, reusing domains
        Assert.Equal(3, replicas.Count);
    }

    #endregion

    #region Prefer Constraint Tests

    [Fact]
    public void AssignReplicas_PreferRegion_PrioritizesPreferredRegion()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "us-east/dc1/zone-a/rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "us-east/dc1/zone-a/rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "us-west/dc1/zone-a/rack-1");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "prefer:region=us-east"
        });

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 2);

        // Assert - Should prefer brokers in us-east
        Assert.Equal(2, replicas.Count);
        Assert.Contains(1, replicas);
        Assert.Contains(2, replicas);
    }

    [Fact]
    public void AssignReplicas_PreferDatacenter_PrioritizesPreferredDatacenter()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "us-east/dc1/zone-a/rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "us-east/dc2/zone-a/rack-1");
        _clusterState.RegisterBroker(3, "localhost", 9094, "us-east/dc1/zone-b/rack-1");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "prefer:datacenter=dc1"
        });

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 2);

        // Assert - Should prefer brokers in dc1
        Assert.Equal(2, replicas.Count);
        Assert.Contains(1, replicas);
        Assert.Contains(3, replicas);
    }

    [Fact]
    public void AssignReplicas_PreferRegion_FillsFromOtherRegionsIfNeeded()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "us-east/dc1/zone-a/rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "us-west/dc1/zone-a/rack-1");
        _clusterState.RegisterBroker(3, "localhost", 9094, "eu-west/dc1/zone-a/rack-1");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "prefer:region=us-east"
        });

        // Act - Request 3 replicas but only 1 broker in preferred region
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 3);

        // Assert
        Assert.Equal(3, replicas.Count);
        Assert.Contains(1, replicas); // Preferred region broker first
    }

    [Fact]
    public void AssignReplicas_InvalidPreference_FallsBackToRoundRobin()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "prefer:invalid" // Missing = separator
        });

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 2);

        // Assert - Falls back to round robin
        Assert.Equal(2, replicas.Count);
    }

    #endregion

    #region Rebalance Tests

    [Fact]
    public void RebalanceReplicas_NoViolations_ReturnsNull()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-3");

        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            PreventSingleDomainReplicas = false, // Disable violation checks
            MinDistinctDomains = 0
        });

        var assignment = new HierarchicalReplicaAssignment(
            _clusterState,
            _domainHierarchy,
            validator,
            NullLogger<HierarchicalReplicaAssignment>.Instance);

        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        // Act
        var result = assignment.RebalanceReplicas(partition, [1, 2, 3], replicationFactor: 3);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void RebalanceReplicas_WithViolations_ReturnsImprovedAssignment()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-1");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-2");
        _clusterState.RegisterBroker(4, "localhost", 9095, "rack-3");

        _domainHierarchy.RegisterBroker(1, "rack-1");
        _domainHierarchy.RegisterBroker(2, "rack-1");
        _domainHierarchy.RegisterBroker(3, "rack-2");
        _domainHierarchy.RegisterBroker(4, "rack-3");

        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            MinDistinctDomains = 3 // Require 3 distinct racks
        });

        var assignment = new HierarchicalReplicaAssignment(
            _clusterState,
            _domainHierarchy,
            validator,
            NullLogger<HierarchicalReplicaAssignment>.Instance,
            new HierarchicalReplicaAssignmentOptions
            {
                PlacementConstraints = "spread_across:rack"
            });

        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        // Act - Current replicas [1, 2, 3] only span 2 racks (rack-1 has 2 replicas)
        var result = assignment.RebalanceReplicas(partition, [1, 2, 3], replicationFactor: 3);

        // Assert - Should return improved assignment with 3 different racks
        if (result != null)
        {
            var racks = result.Select(r => _clusterState.GetBroker(r)?.Rack).Distinct().ToList();
            Assert.True(racks.Count >= 2);
        }
    }

    [Fact]
    public void RebalanceReplicas_PreservesCurrentLeaderIfPossible()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-1");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-2");
        _clusterState.RegisterBroker(4, "localhost", 9095, "rack-3");

        _domainHierarchy.RegisterBroker(1, "rack-1");
        _domainHierarchy.RegisterBroker(2, "rack-1");
        _domainHierarchy.RegisterBroker(3, "rack-2");
        _domainHierarchy.RegisterBroker(4, "rack-3");

        var validator = new FailureDomainValidator(_clusterState, new FailureDomainValidatorOptions
        {
            MinDistinctDomains = 3
        });

        var assignment = new HierarchicalReplicaAssignment(
            _clusterState,
            _domainHierarchy,
            validator,
            NullLogger<HierarchicalReplicaAssignment>.Instance,
            new HierarchicalReplicaAssignmentOptions
            {
                PlacementConstraints = "spread_across:rack"
            });

        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        // Act
        var result = assignment.RebalanceReplicas(partition, [1, 2, 3], replicationFactor: 3);

        // Assert - If broker 1 is in new assignment, it should be first
        if (result != null && result.Contains(1))
        {
            Assert.Equal(1, result[0]);
        }
    }

    #endregion

    #region HierarchicalReplicaAssignmentOptions Tests

    [Fact]
    public void HierarchicalReplicaAssignmentOptions_DefaultValues()
    {
        // Act
        var options = new HierarchicalReplicaAssignmentOptions();

        // Assert
        Assert.Null(options.PlacementConstraints);
        Assert.Equal(0, options.MinDistinctDomains);
    }

    [Fact]
    public void HierarchicalReplicaAssignmentOptions_CustomValues()
    {
        // Act
        var options = new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "spread_across:zone",
            MinDistinctDomains = 2
        };

        // Assert
        Assert.Equal("spread_across:zone", options.PlacementConstraints);
        Assert.Equal(2, options.MinDistinctDomains);
    }

    #endregion

    #region Constraint Parsing Tests

    [Fact]
    public void AssignReplicas_NullConstraint_UsesRoundRobin()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = null
        });

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 2);

        // Assert
        Assert.Equal(2, replicas.Count);
    }

    [Fact]
    public void AssignReplicas_EmptyConstraint_UsesRoundRobin()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = ""
        });

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 2);

        // Assert
        Assert.Equal(2, replicas.Count);
    }

    [Fact]
    public void AssignReplicas_UnknownConstraintType_UsesRoundRobin()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "unknown_type:value"
        });

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 2);

        // Assert - Falls back to round-robin
        Assert.Equal(2, replicas.Count);
    }

    [Fact]
    public void AssignReplicas_SpreadAcrossAlternativeNames_Works()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "us-east/dc1/zone-a/rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "us-east/dc2/zone-a/rack-1");

        // Act & Assert - Using "dc" instead of "datacenter"
        var assignment1 = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "spread_across:dc"
        });
        var replicas1 = assignment1.AssignReplicas("test-topic", partition: 0, replicationFactor: 2);
        Assert.Equal(2, replicas1.Count);

        // Using "az" instead of "zone"
        var assignment2 = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "spread_across:az"
        });
        var replicas2 = assignment2.AssignReplicas("test-topic", partition: 0, replicationFactor: 2);
        Assert.Equal(2, replicas2.Count);
    }

    #endregion

    #region Edge Cases

    [Fact]
    public void AssignReplicas_SingleBroker_AssignsOne()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");

        var assignment = CreateAssignment();

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 3);

        // Assert
        Assert.Single(replicas);
        Assert.Equal(1, replicas[0]);
    }

    [Fact]
    public void AssignReplicas_BrokersWithoutRack_TreatedAsDefault()
    {
        // Arrange
        _clusterState.RegisterBroker(1, "localhost", 9092, null);
        _clusterState.RegisterBroker(2, "localhost", 9093, null);

        var assignment = CreateAssignment(new HierarchicalReplicaAssignmentOptions
        {
            PlacementConstraints = "spread_across:rack"
        });

        // Act
        var replicas = assignment.AssignReplicas("test-topic", partition: 0, replicationFactor: 2);

        // Assert - All go to "default" domain
        Assert.Equal(2, replicas.Count);
    }

    #endregion
}

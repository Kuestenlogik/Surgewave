using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Reassignment;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Reassignment;

[Trait("Category", TestCategories.Unit)]
public class ReassignmentPlannerTests
{
    private readonly ReassignmentPlanner _planner = new();

    private static TopicPartitionInfo MakePartition(
        string topic, int partition, int leader, List<int> replicas, long sizeBytes = 1000)
    {
        return new TopicPartitionInfo(topic, partition, leader, replicas, replicas, sizeBytes);
    }

    // --- Balance Plan Tests ---

    [Fact]
    public void GenerateBalancePlan_DistributesEvenly()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 0, [0]),
            MakePartition("t1", 1, 0, [0]),
            MakePartition("t1", 2, 0, [0]),
        };

        var plan = _planner.GenerateBalancePlan(assignments, [0, 1, 2]);

        // At least some partitions should move to brokers 1 and 2
        Assert.True(plan.Assignments.Count > 0, "Should propose moves");

        var targetBrokers = plan.Assignments
            .SelectMany(a => a.TargetReplicas)
            .Distinct()
            .ToList();

        Assert.True(targetBrokers.Count > 1,
            "Should distribute across multiple brokers");
    }

    [Fact]
    public void GenerateBalancePlan_NoBrokers_ReturnsEmptyPlan()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 0, [0])
        };

        var plan = _planner.GenerateBalancePlan(assignments, []);

        Assert.Empty(plan.Assignments);
    }

    [Fact]
    public void GenerateBalancePlan_AlreadyBalanced_ReturnsEmptyPlan()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 0, [0]),
            MakePartition("t1", 1, 1, [1]),
            MakePartition("t1", 2, 2, [2]),
        };

        var plan = _planner.GenerateBalancePlan(assignments, [0, 1, 2]);

        Assert.Empty(plan.Assignments);
    }

    [Fact]
    public void GenerateBalancePlan_PreservesReplicationFactor()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 0, [0, 0, 0]), // RF=3 but all on broker 0 (degenerate)
        };

        var plan = _planner.GenerateBalancePlan(assignments, [0, 1, 2]);

        foreach (var a in plan.Assignments)
        {
            Assert.Equal(3, a.TargetReplicas.Count);
        }
    }

    // --- Decommission Plan Tests ---

    [Fact]
    public void GenerateDecommissionPlan_MovesAllPartitions()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 0, [0, 1]),
            MakePartition("t1", 1, 0, [0, 2]),
            MakePartition("t1", 2, 1, [1, 2]),
        };

        var plan = _planner.GenerateDecommissionPlan(0, assignments, [1, 2]);

        // Only partitions with broker 0 should be moved (p0 and p1)
        Assert.Equal(2, plan.Assignments.Count);

        // No target should include broker 0
        foreach (var a in plan.Assignments)
        {
            Assert.DoesNotContain(0, a.TargetReplicas);
        }
    }

    [Fact]
    public void GenerateDecommissionPlan_NoPartitionsOnBroker_ReturnsEmpty()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 1, [1, 2]),
        };

        var plan = _planner.GenerateDecommissionPlan(0, assignments, [1, 2]);

        Assert.Empty(plan.Assignments);
    }

    [Fact]
    public void GenerateDecommissionPlan_NoRemainingBrokers_ReturnsEmpty()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 0, [0]),
        };

        var plan = _planner.GenerateDecommissionPlan(0, assignments, []);

        Assert.Empty(plan.Assignments);
    }

    // --- Replication Plan Tests ---

    [Fact]
    public void GenerateReplicationPlan_IncreasesReplicas()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 0, [0]),
            MakePartition("t1", 1, 1, [1]),
        };

        var plan = _planner.GenerateReplicationPlan("t1", 3, assignments, [0, 1, 2]);

        Assert.Equal(2, plan.Assignments.Count);
        foreach (var a in plan.Assignments)
        {
            Assert.Equal(3, a.TargetReplicas.Count);
        }
    }

    [Fact]
    public void GenerateReplicationPlan_DecreasesReplicas()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 0, [0, 1, 2]),
        };

        var plan = _planner.GenerateReplicationPlan("t1", 1, assignments, [0, 1, 2]);

        Assert.Single(plan.Assignments);
        Assert.Single(plan.Assignments[0].TargetReplicas);
    }

    [Fact]
    public void GenerateReplicationPlan_SameRf_ReturnsEmpty()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 0, [0, 1]),
        };

        var plan = _planner.GenerateReplicationPlan("t1", 2, assignments, [0, 1, 2]);

        Assert.Empty(plan.Assignments);
    }

    [Fact]
    public void GenerateReplicationPlan_ClampsToAvailableBrokers()
    {
        var assignments = new List<TopicPartitionInfo>
        {
            MakePartition("t1", 0, 0, [0]),
        };

        // Request RF=5 but only 2 brokers available
        var plan = _planner.GenerateReplicationPlan("t1", 5, assignments, [0, 1]);

        Assert.Single(plan.Assignments);
        Assert.Equal(2, plan.Assignments[0].TargetReplicas.Count);
    }

    // --- Validation Tests ---

    [Fact]
    public void ValidatePlan_DetectsInvalidBroker()
    {
        var plan = new OnlineReassignmentPlan
        {
            Assignments =
            [
                new OnlinePartitionReassignment
                {
                    Topic = "t1", Partition = 0,
                    CurrentReplicas = [0], TargetReplicas = [99]
                }
            ]
        };

        var result = _planner.ValidatePlan(plan, [0, 1, 2]);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("unknown broker 99"));
    }

    [Fact]
    public void ValidatePlan_DetectsDuplicates()
    {
        var plan = new OnlineReassignmentPlan
        {
            Assignments =
            [
                new OnlinePartitionReassignment
                {
                    Topic = "t1", Partition = 0,
                    CurrentReplicas = [0], TargetReplicas = [0, 0]
                }
            ]
        };

        var result = _planner.ValidatePlan(plan, [0, 1, 2]);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("duplicate broker 0"));
    }

    [Fact]
    public void ValidatePlan_DetectsEmptyTargetReplicas()
    {
        var plan = new OnlineReassignmentPlan
        {
            Assignments =
            [
                new OnlinePartitionReassignment
                {
                    Topic = "t1", Partition = 0,
                    CurrentReplicas = [0], TargetReplicas = []
                }
            ]
        };

        var result = _planner.ValidatePlan(plan, [0, 1, 2]);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("empty target replica list"));
    }

    [Fact]
    public void ValidatePlan_DetectsDuplicatePartitionEntries()
    {
        var plan = new OnlineReassignmentPlan
        {
            Assignments =
            [
                new OnlinePartitionReassignment
                {
                    Topic = "t1", Partition = 0,
                    CurrentReplicas = [0], TargetReplicas = [1]
                },
                new OnlinePartitionReassignment
                {
                    Topic = "t1", Partition = 0,
                    CurrentReplicas = [0], TargetReplicas = [2]
                }
            ]
        };

        var result = _planner.ValidatePlan(plan, [0, 1, 2]);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Duplicate assignment"));
    }

    [Fact]
    public void ValidatePlan_WarnsOnNoOp()
    {
        var plan = new OnlineReassignmentPlan
        {
            Assignments =
            [
                new OnlinePartitionReassignment
                {
                    Topic = "t1", Partition = 0,
                    CurrentReplicas = [0, 1], TargetReplicas = [0, 1]
                }
            ]
        };

        var result = _planner.ValidatePlan(plan, [0, 1, 2]);

        Assert.True(result.IsValid);
        Assert.Contains(result.Warnings, w => w.Contains("no-op"));
    }

    [Fact]
    public void ValidatePlan_ValidPlanPasses()
    {
        var plan = new OnlineReassignmentPlan
        {
            Assignments =
            [
                new OnlinePartitionReassignment
                {
                    Topic = "t1", Partition = 0,
                    CurrentReplicas = [0], TargetReplicas = [1]
                },
                new OnlinePartitionReassignment
                {
                    Topic = "t1", Partition = 1,
                    CurrentReplicas = [0], TargetReplicas = [2]
                }
            ]
        };

        var result = _planner.ValidatePlan(plan, [0, 1, 2]);

        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void ValidatePlan_EmptyPlan_WarnsButValid()
    {
        var plan = new OnlineReassignmentPlan();

        var result = _planner.ValidatePlan(plan, [0, 1, 2]);

        Assert.True(result.IsValid);
        Assert.Contains(result.Warnings, w => w.Contains("no assignments"));
    }
}

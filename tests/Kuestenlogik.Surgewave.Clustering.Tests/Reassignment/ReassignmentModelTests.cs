using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Reassignment;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Reassignment;

[Trait("Category", TestCategories.Unit)]
public class ReassignmentModelTests
{
    [Fact]
    public void OnlineReassignmentPlan_Defaults()
    {
        var plan = new OnlineReassignmentPlan();

        Assert.NotNull(plan.Id);
        Assert.Equal(32, plan.Id.Length); // Guid.ToString("N") is 32 chars
        Assert.Empty(plan.Assignments);
        Assert.Equal(ReassignmentPlanStatus.Proposed, plan.Status);
        Assert.Equal(50_000_000, plan.ThrottleRateBytesPerSec);
        Assert.Null(plan.Description);
        Assert.Null(plan.StartedAt);
        Assert.Null(plan.CompletedAt);
    }

    [Fact]
    public void OnlinePartitionReassignment_ProgressTracking()
    {
        var assignment = new OnlinePartitionReassignment
        {
            Topic = "test-topic",
            Partition = 3,
            CurrentReplicas = [0, 1],
            TargetReplicas = [2, 3],
            TotalBytes = 1000
        };

        Assert.Equal(ReassignmentStatus.Pending, assignment.Status);
        Assert.Equal(0.0, assignment.Progress);
        Assert.Equal(0, assignment.BytesCopied);

        // Simulate progress
        assignment.BytesCopied = 500;
        assignment.Progress = 0.5;
        assignment.Status = ReassignmentStatus.Syncing;

        Assert.Equal(0.5, assignment.Progress);
        Assert.Equal(500, assignment.BytesCopied);
    }

    [Fact]
    public void ReassignmentConfig_Defaults()
    {
        var config = new ReassignmentConfig();

        Assert.Equal(50_000_000, config.DefaultThrottleRateBytesPerSec);
        Assert.Equal(5, config.MaxConcurrentReassignments);
        Assert.Equal(TimeSpan.FromSeconds(10), config.ProgressCheckInterval);
        Assert.False(config.AutoRebalanceOnBrokerJoin);
    }

    [Fact]
    public void ReassignmentPlanStatus_AllValues()
    {
        var values = Enum.GetValues<ReassignmentPlanStatus>();

        Assert.Contains(ReassignmentPlanStatus.Proposed, values);
        Assert.Contains(ReassignmentPlanStatus.Executing, values);
        Assert.Contains(ReassignmentPlanStatus.Completed, values);
        Assert.Contains(ReassignmentPlanStatus.Failed, values);
        Assert.Contains(ReassignmentPlanStatus.Cancelled, values);
        Assert.Equal(5, values.Length);
    }

    [Fact]
    public void ReassignmentStatus_AllValues()
    {
        var values = Enum.GetValues<ReassignmentStatus>();

        Assert.Contains(ReassignmentStatus.Pending, values);
        Assert.Contains(ReassignmentStatus.Adding, values);
        Assert.Contains(ReassignmentStatus.Syncing, values);
        Assert.Contains(ReassignmentStatus.Completing, values);
        Assert.Contains(ReassignmentStatus.Completed, values);
        Assert.Contains(ReassignmentStatus.Failed, values);
        Assert.Contains(ReassignmentStatus.Cancelled, values);
        Assert.Equal(7, values.Length);
    }

    [Fact]
    public void TopicPartitionInfo_Properties()
    {
        var info = new TopicPartitionInfo(
            "my-topic", 5, Leader: 1,
            Replicas: [1, 2, 3],
            Isr: [1, 2],
            SizeBytes: 1024 * 1024);

        Assert.Equal("my-topic", info.Topic);
        Assert.Equal(5, info.Partition);
        Assert.Equal(1, info.Leader);
        Assert.Equal(3, info.Replicas.Count);
        Assert.Equal(2, info.Isr.Count);
        Assert.Equal(1024 * 1024, info.SizeBytes);
    }

    [Fact]
    public void ReassignmentResult_Properties()
    {
        var result = new ReassignmentResult(
            PlanId: "abc123",
            Status: ReassignmentPlanStatus.Completed,
            TotalPartitions: 10,
            Completed: 8,
            Failed: 2,
            Duration: TimeSpan.FromMinutes(5),
            TotalBytesCopied: 1024 * 1024 * 100);

        Assert.Equal("abc123", result.PlanId);
        Assert.Equal(ReassignmentPlanStatus.Completed, result.Status);
        Assert.Equal(10, result.TotalPartitions);
        Assert.Equal(8, result.Completed);
        Assert.Equal(2, result.Failed);
        Assert.Equal(TimeSpan.FromMinutes(5), result.Duration);
        Assert.Equal(1024 * 1024 * 100, result.TotalBytesCopied);
    }

    [Fact]
    public void ReassignmentValidation_Properties()
    {
        var valid = new ReassignmentValidation(
            IsValid: true,
            Errors: [],
            Warnings: ["Some warning"]);

        Assert.True(valid.IsValid);
        Assert.Empty(valid.Errors);
        Assert.Single(valid.Warnings);

        var invalid = new ReassignmentValidation(
            IsValid: false,
            Errors: ["Bad broker"],
            Warnings: []);

        Assert.False(invalid.IsValid);
        Assert.Single(invalid.Errors);
    }
}

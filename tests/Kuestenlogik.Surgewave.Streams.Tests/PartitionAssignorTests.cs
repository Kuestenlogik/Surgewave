using Kuestenlogik.Surgewave.Streams.Runtime;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

[Trait("Category", "Unit")]
public class PartitionAssignorTests
{
    private static List<TopicPartition> CreatePartitions(string topic, int count)
    {
        return Enumerable.Range(0, count)
            .Select(i => new TopicPartition(topic, i))
            .ToList();
    }

    // --- RoundRobinAssignor ---

    [Fact]
    public void RoundRobin_DistributesEvenly_AcrossThreads()
    {
        var assignor = RoundRobinAssignor.Instance;
        var partitions = CreatePartitions("test-topic", 6);

        var result = assignor.Assign(partitions, 3, null);

        Assert.Equal(3, result.Count);
        Assert.Equal(2, result[0].Count);
        Assert.Equal(2, result[1].Count);
        Assert.Equal(2, result[2].Count);
    }

    [Fact]
    public void RoundRobin_HandlesUnevenDistribution()
    {
        var assignor = RoundRobinAssignor.Instance;
        var partitions = CreatePartitions("test-topic", 7);

        var result = assignor.Assign(partitions, 3, null);

        // 7 / 3 = 3, 2, 2
        Assert.Equal(3, result[0].Count);
        Assert.Equal(2, result[1].Count);
        Assert.Equal(2, result[2].Count);
    }

    [Fact]
    public void RoundRobin_SingleThread_AllPartitions()
    {
        var assignor = RoundRobinAssignor.Instance;
        var partitions = CreatePartitions("test-topic", 4);

        var result = assignor.Assign(partitions, 1, null);

        Assert.Single(result);
        Assert.Equal(4, result[0].Count);
    }

    [Fact]
    public void RoundRobin_IgnoresPreviousAssignment()
    {
        var assignor = RoundRobinAssignor.Instance;
        var partitions = CreatePartitions("test-topic", 4);

        var previous = new Dictionary<TopicPartition, int>
        {
            [partitions[0]] = 1,
            [partitions[1]] = 0,
            [partitions[2]] = 1,
            [partitions[3]] = 0
        };

        var result = assignor.Assign(partitions, 2, previous);

        // Round-robin always does 0,1,0,1 regardless of previous
        Assert.Equal(2, result[0].Count);
        Assert.Equal(2, result[1].Count);
        Assert.Contains(partitions[0], result[0]);
        Assert.Contains(partitions[1], result[1]);
    }

    [Fact]
    public void RoundRobin_EmptyPartitions_ReturnsEmptyLists()
    {
        var assignor = RoundRobinAssignor.Instance;
        var result = assignor.Assign([], 3, null);

        Assert.Equal(3, result.Count);
        Assert.Empty(result[0]);
        Assert.Empty(result[1]);
        Assert.Empty(result[2]);
    }

    // --- StickyAssignor ---

    [Fact]
    public void Sticky_NoPreviousAssignment_FallsBackToRoundRobin()
    {
        var assignor = StickyAssignor.Instance;
        var partitions = CreatePartitions("test-topic", 6);

        var result = assignor.Assign(partitions, 3, null);

        Assert.Equal(3, result.Count);
        Assert.Equal(2, result[0].Count);
        Assert.Equal(2, result[1].Count);
        Assert.Equal(2, result[2].Count);
    }

    [Fact]
    public void Sticky_RetainsPreviousAssignment_WhenBalanced()
    {
        var assignor = StickyAssignor.Instance;
        var partitions = CreatePartitions("test-topic", 4);

        // Previous: each thread had 2 partitions
        var previous = new Dictionary<TopicPartition, int>
        {
            [partitions[0]] = 0,
            [partitions[1]] = 0,
            [partitions[2]] = 1,
            [partitions[3]] = 1
        };

        var result = assignor.Assign(partitions, 2, previous);

        // Should retain exact same assignment (already balanced)
        Assert.Contains(partitions[0], result[0]);
        Assert.Contains(partitions[1], result[0]);
        Assert.Contains(partitions[2], result[1]);
        Assert.Contains(partitions[3], result[1]);
    }

    [Fact]
    public void Sticky_MinimizesMovement_OnScaleUp()
    {
        var assignor = StickyAssignor.Instance;
        var partitions = CreatePartitions("test-topic", 6);

        // Previously 2 threads, each had 3 partitions
        var previous = new Dictionary<TopicPartition, int>
        {
            [partitions[0]] = 0,
            [partitions[1]] = 0,
            [partitions[2]] = 0,
            [partitions[3]] = 1,
            [partitions[4]] = 1,
            [partitions[5]] = 1
        };

        // Scale up to 3 threads
        var result = assignor.Assign(partitions, 3, previous);

        // Each thread should have 2 partitions (6/3 = 2)
        Assert.Equal(2, result[0].Count);
        Assert.Equal(2, result[1].Count);
        Assert.Equal(2, result[2].Count);

        // The moved partitions should go to thread 2 (new thread)
        // Threads 0 and 1 should retain most of their partitions
        var totalRetained = result[0].Count(p => previous.TryGetValue(p, out var t) && t == 0)
                          + result[1].Count(p => previous.TryGetValue(p, out var t) && t == 1);
        Assert.True(totalRetained >= 4, "At least 4 of 6 partitions should be retained on original threads");
    }

    [Fact]
    public void Sticky_HandlesNewPartitions()
    {
        var assignor = StickyAssignor.Instance;
        var oldPartitions = CreatePartitions("test-topic", 4);
        var newPartitions = CreatePartitions("test-topic", 6); // 2 new partitions added

        var previous = new Dictionary<TopicPartition, int>
        {
            [oldPartitions[0]] = 0,
            [oldPartitions[1]] = 0,
            [oldPartitions[2]] = 1,
            [oldPartitions[3]] = 1
        };

        var result = assignor.Assign(newPartitions, 2, previous);

        // Old partitions should stay on their threads
        Assert.Contains(newPartitions[0], result[0]);
        Assert.Contains(newPartitions[1], result[0]);
        Assert.Contains(newPartitions[2], result[1]);
        Assert.Contains(newPartitions[3], result[1]);

        // New partitions distributed to least loaded
        Assert.Equal(6, result[0].Count + result[1].Count);
    }

    [Fact]
    public void Sticky_HandlesThreadReduction()
    {
        var assignor = StickyAssignor.Instance;
        var partitions = CreatePartitions("test-topic", 6);

        // Previously 3 threads
        var previous = new Dictionary<TopicPartition, int>
        {
            [partitions[0]] = 0,
            [partitions[1]] = 0,
            [partitions[2]] = 1,
            [partitions[3]] = 1,
            [partitions[4]] = 2,
            [partitions[5]] = 2
        };

        // Scale down to 2 threads — partitions from thread 2 must move
        var result = assignor.Assign(partitions, 2, previous);

        Assert.Equal(2, result.Count);
        Assert.Equal(3, result[0].Count);
        Assert.Equal(3, result[1].Count);

        // Thread 0 and 1 should retain their original partitions
        Assert.Contains(partitions[0], result[0]);
        Assert.Contains(partitions[1], result[0]);
        Assert.Contains(partitions[2], result[1]);
        Assert.Contains(partitions[3], result[1]);
    }

    [Fact]
    public void Sticky_MultipleTopics_DistributesCorrectly()
    {
        var assignor = StickyAssignor.Instance;
        var topicA = CreatePartitions("topic-a", 3);
        var topicB = CreatePartitions("topic-b", 3);
        var all = topicA.Concat(topicB).ToList();

        var result = assignor.Assign(all, 3, null);

        // Total should be 6 across 3 threads
        Assert.Equal(6, result.Values.Sum(v => v.Count));
        Assert.Equal(2, result[0].Count);
        Assert.Equal(2, result[1].Count);
        Assert.Equal(2, result[2].Count);
    }
}

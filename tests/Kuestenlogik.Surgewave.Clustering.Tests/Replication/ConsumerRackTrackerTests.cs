using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Replication;

/// <summary>
/// Tests for ConsumerRackTracker.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ConsumerRackTrackerTests
{
    [Fact]
    public void RegisterConsumer_WithRack_TracksRack()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        // Act
        tracker.RegisterConsumer("consumer-1", "rack-1");
        tracker.RecordFetch(partition, "consumer-1");

        // Assert
        var dominantRack = tracker.GetDominantRack(partition);
        Assert.Equal("rack-1", dominantRack);
    }

    [Fact]
    public void RegisterConsumer_NullRack_DoesNotTrack()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        // Act
        tracker.RegisterConsumer("consumer-1", null);
        tracker.RecordFetch(partition, "consumer-1");

        // Assert
        var dominantRack = tracker.GetDominantRack(partition);
        Assert.Null(dominantRack);
    }

    [Fact]
    public void RegisterConsumer_EmptyRack_DoesNotTrack()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        // Act
        tracker.RegisterConsumer("consumer-1", "");
        tracker.RecordFetch(partition, "consumer-1");

        // Assert
        var dominantRack = tracker.GetDominantRack(partition);
        Assert.Null(dominantRack);
    }

    [Fact]
    public void UnregisterConsumer_RemovesFromTracking()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };
        tracker.RegisterConsumer("consumer-1", "rack-1");
        tracker.RecordFetch(partition, "consumer-1");

        // Act
        tracker.UnregisterConsumer("consumer-1");

        // Assert - After cleanup delay
        var dominantRack = tracker.GetDominantRack(partition);
        Assert.Null(dominantRack);
    }

    [Fact]
    public void GetDominantRack_MultipleConsumers_ReturnsMostCommon()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        tracker.RegisterConsumer("consumer-1", "rack-1");
        tracker.RegisterConsumer("consumer-2", "rack-1");
        tracker.RegisterConsumer("consumer-3", "rack-2");

        tracker.RecordFetch(partition, "consumer-1");
        tracker.RecordFetch(partition, "consumer-2");
        tracker.RecordFetch(partition, "consumer-3");

        // Act
        var dominantRack = tracker.GetDominantRack(partition);

        // Assert
        Assert.Equal("rack-1", dominantRack);
    }

    [Fact]
    public void GetDominantRack_NoFetches_ReturnsNull()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        // Act
        var dominantRack = tracker.GetDominantRack(partition);

        // Assert
        Assert.Null(dominantRack);
    }

    [Fact]
    public void GetRackConsumerCounts_ReturnsCorrectCounts()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        tracker.RegisterConsumer("consumer-1", "rack-1");
        tracker.RegisterConsumer("consumer-2", "rack-1");
        tracker.RegisterConsumer("consumer-3", "rack-2");
        tracker.RegisterConsumer("consumer-4", "rack-2");
        tracker.RegisterConsumer("consumer-5", "rack-2");

        foreach (var id in new[] { "consumer-1", "consumer-2", "consumer-3", "consumer-4", "consumer-5" })
        {
            tracker.RecordFetch(partition, id);
        }

        // Act
        var counts = tracker.GetRackConsumerCounts(partition);

        // Assert
        Assert.Equal(2, counts["rack-1"]);
        Assert.Equal(3, counts["rack-2"]);
    }

    [Fact]
    public void GetActiveRacks_ReturnsDistinctRacks()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        tracker.RegisterConsumer("consumer-1", "rack-1");
        tracker.RegisterConsumer("consumer-2", "rack-2");
        tracker.RegisterConsumer("consumer-3", "rack-1");

        tracker.RecordFetch(partition, "consumer-1");
        tracker.RecordFetch(partition, "consumer-2");
        tracker.RecordFetch(partition, "consumer-3");

        // Act
        var activeRacks = tracker.GetActiveRacks(partition).ToList();

        // Assert
        Assert.Equal(2, activeRacks.Count);
        Assert.Contains("rack-1", activeRacks);
        Assert.Contains("rack-2", activeRacks);
    }

    [Fact]
    public void RecordFetch_UnknownConsumer_DoesNotThrow()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        // Act & Assert - Should not throw
        tracker.RecordFetch(partition, "unknown-consumer");
        var dominantRack = tracker.GetDominantRack(partition);
        Assert.Null(dominantRack);
    }

    [Fact]
    public void RecordFetch_UpdatesLastSeenTime()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        tracker.RegisterConsumer("consumer-1", "rack-1");

        // Act - Record multiple fetches
        tracker.RecordFetch(partition, "consumer-1");
        Thread.Sleep(10);
        tracker.RecordFetch(partition, "consumer-1");

        // Assert - Consumer should still be active
        var dominantRack = tracker.GetDominantRack(partition);
        Assert.Equal("rack-1", dominantRack);
    }

    [Fact]
    public void DifferentPartitions_TrackSeparately()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition1 = new TopicPartition { Topic = "test", Partition = 0 };
        var partition2 = new TopicPartition { Topic = "test", Partition = 1 };

        tracker.RegisterConsumer("consumer-1", "rack-1");
        tracker.RegisterConsumer("consumer-2", "rack-2");

        tracker.RecordFetch(partition1, "consumer-1");
        tracker.RecordFetch(partition2, "consumer-2");

        // Act
        var dominant1 = tracker.GetDominantRack(partition1);
        var dominant2 = tracker.GetDominantRack(partition2);

        // Assert
        Assert.Equal("rack-1", dominant1);
        Assert.Equal("rack-2", dominant2);
    }

    [Fact]
    public void Cleanup_RemovesStaleConsumers()
    {
        // Arrange - Very short timeout for testing
        var tracker = new ConsumerRackTracker(TimeSpan.FromMilliseconds(50));
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        tracker.RegisterConsumer("consumer-1", "rack-1");
        tracker.RecordFetch(partition, "consumer-1");

        // Wait for consumer to become stale
        Thread.Sleep(100);

        // Act
        tracker.Cleanup();

        // Assert
        var dominantRack = tracker.GetDominantRack(partition);
        Assert.Null(dominantRack);
    }

    [Fact]
    public void GetRackConsumerCounts_NoPartition_ReturnsEmpty()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "nonexistent", Partition = 999 };

        // Act
        var counts = tracker.GetRackConsumerCounts(partition);

        // Assert
        Assert.Empty(counts);
    }

    [Fact]
    public void GetActiveRacks_NoPartition_ReturnsEmpty()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "nonexistent", Partition = 999 };

        // Act
        var racks = tracker.GetActiveRacks(partition);

        // Assert
        Assert.Empty(racks);
    }

    [Fact]
    public void Constructor_DefaultTimeout_FiveMinutes()
    {
        // Arrange & Act
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        tracker.RegisterConsumer("consumer-1", "rack-1");
        tracker.RecordFetch(partition, "consumer-1");

        // Assert - Consumer should still be active (default 5 min timeout)
        var dominantRack = tracker.GetDominantRack(partition);
        Assert.Equal("rack-1", dominantRack);
    }

    [Fact]
    public void MultipleFetches_SameConsumer_CountsOnce()
    {
        // Arrange
        var tracker = new ConsumerRackTracker();
        var partition = new TopicPartition { Topic = "test", Partition = 0 };

        tracker.RegisterConsumer("consumer-1", "rack-1");
        tracker.RecordFetch(partition, "consumer-1");
        tracker.RecordFetch(partition, "consumer-1");
        tracker.RecordFetch(partition, "consumer-1");

        // Act
        var counts = tracker.GetRackConsumerCounts(partition);

        // Assert - Same consumer only counted once
        Assert.Equal(1, counts["rack-1"]);
    }
}

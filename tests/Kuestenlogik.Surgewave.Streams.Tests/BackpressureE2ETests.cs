using Kuestenlogik.Surgewave.Streams.Runtime;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// End-to-end integration tests for BackpressureCoordinator and PartitionBackpressureTracker
/// used together, covering concurrent access, stress patterns, and reset semantics.
/// </summary>
public sealed class BackpressureE2ETests
{
    // ── helpers ───────────────────────────────────────────────────────────────

    private static BackpressureConfig MakeConfig(
        int max = 100,
        double high = 0.8,
        double low = 0.5,
        bool pause = true)
        => new()
        {
            MaxBufferedRecords = max,
            HighWatermarkRatio = high,
            LowWatermarkRatio = low,
            PauseConsumerOnHighWatermark = pause
        };

    // ── Coordinator + Tracker used together ──────────────────────────────────

    [Fact]
    public void E2E_CoordinatorAndTracker_PauseResumePerPartition()
    {
        var config = MakeConfig(max: 10, high: 0.8, low: 0.5);
        var coordinator = new BackpressureCoordinator(config);
        var tracker = new PartitionBackpressureTracker(config);

        int pauseCount = 0;
        int resumeCount = 0;
        coordinator.OnHighWatermarkReached += () => pauseCount++;
        coordinator.OnLowWatermarkReached += () => resumeCount++;

        // Fill partition 0 to high watermark
        for (int i = 0; i < 8; i++)
            tracker.RecordBuffered("orders", 0);

        Assert.True(tracker.ShouldPause("orders", 0));

        // Coordinator mirrors partition state
        coordinator.OnRecordBuffered(tracker.GetCount("orders", 0));
        Assert.Equal(1, pauseCount);
        Assert.True(coordinator.IsPaused);

        // Partition 1 still below high watermark — coordinator stays paused
        for (int i = 0; i < 3; i++)
            tracker.RecordBuffered("orders", 1);

        Assert.False(tracker.ShouldPause("orders", 1));

        // Drain partition 0 below low watermark
        for (int i = 0; i < 4; i++)
            tracker.RecordProcessed("orders", 0); // 4 remaining

        coordinator.OnRecordProcessed(tracker.GetCount("orders", 0));
        Assert.Equal(1, resumeCount);
        Assert.False(coordinator.IsPaused);
    }

    [Fact]
    public void E2E_MultiplePartitions_IndependentWatermarks()
    {
        var config = MakeConfig(max: 10, high: 0.8, low: 0.5);
        var tracker = new PartitionBackpressureTracker(config);

        // Fill partition 0 past high watermark
        for (int i = 0; i < 9; i++)
            tracker.RecordBuffered("topic", 0);

        // Fill partition 1 only partially
        for (int i = 0; i < 4; i++)
            tracker.RecordBuffered("topic", 1);

        // Fill partition 2 past high watermark
        for (int i = 0; i < 8; i++)
            tracker.RecordBuffered("topic", 2);

        Assert.True(tracker.ShouldPause("topic", 0));
        Assert.False(tracker.ShouldPause("topic", 1));
        Assert.True(tracker.ShouldPause("topic", 2));

        // Drain partition 0 to low watermark
        for (int i = 0; i < 5; i++)
            tracker.RecordProcessed("topic", 0); // 4 remaining

        Assert.True(tracker.ShouldResume("topic", 0));
        Assert.False(tracker.ShouldResume("topic", 2)); // still above low watermark
    }

    [Fact]
    public void E2E_StressTest_1000BufferProcessCycles_NeverGoesNegative()
    {
        var config = MakeConfig(max: 50, high: 0.8, low: 0.5);
        var coordinator = new BackpressureCoordinator(config);
        var tracker = new PartitionBackpressureTracker(config);

        int highCount = 0;
        int lowCount = 0;
        coordinator.OnHighWatermarkReached += () => highCount++;
        coordinator.OnLowWatermarkReached += () => lowCount++;

        const int cycles = 1000;
        int simulated = 0;

        for (int i = 0; i < cycles; i++)
        {
            // Buffer 45 records
            for (int j = 0; j < 45; j++)
            {
                tracker.RecordBuffered("stress", 0);
                simulated++;
            }

            coordinator.OnRecordBuffered(tracker.GetCount("stress", 0));

            // Process 45 records
            for (int j = 0; j < 45; j++)
            {
                tracker.RecordProcessed("stress", 0);
                simulated--;
            }

            coordinator.OnRecordProcessed(tracker.GetCount("stress", 0));
        }

        // Counter must never go negative
        Assert.Equal(0, tracker.GetCount("stress", 0));
        Assert.Equal(0, simulated);
        // Watermarks must have fired a consistent number of times
        Assert.True(highCount > 0, "High watermark should have fired during stress test");
        Assert.Equal(highCount, lowCount);
    }

    [Fact]
    public async Task E2E_ConcurrentAccess_NoDuplicateHighWatermarkEvents()
    {
        var config = MakeConfig(max: 100, high: 0.8, low: 0.5);
        var coordinator = new BackpressureCoordinator(config);

        int eventCount = 0;
        coordinator.OnHighWatermarkReached += () => Interlocked.Increment(ref eventCount);

        // 10 tasks each buffering at the high watermark simultaneously
        var tasks = Enumerable.Range(0, 10)
            .Select(_ => Task.Run(() =>
            {
                // All hit at exactly the high watermark (80 out of 100)
                coordinator.OnRecordBuffered(80);
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        // Due to Interlocked.CompareExchange only one thread wins the transition
        Assert.Equal(1, eventCount);
        Assert.True(coordinator.IsPaused);
    }

    [Fact]
    public async Task E2E_ConcurrentAccess_NoDuplicateLowWatermarkEvents()
    {
        var config = MakeConfig(max: 100, high: 0.8, low: 0.5);
        var coordinator = new BackpressureCoordinator(config);

        int resumeCount = 0;
        coordinator.OnLowWatermarkReached += () => Interlocked.Increment(ref resumeCount);

        // First, pause the coordinator
        coordinator.OnRecordBuffered(80);
        Assert.True(coordinator.IsPaused);

        // 10 tasks each racing to trigger the low watermark
        var tasks = Enumerable.Range(0, 10)
            .Select(_ => Task.Run(() =>
            {
                coordinator.OnRecordProcessed(40); // below low watermark (50)
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        // Only one thread should win the transition back to unpaused
        Assert.Equal(1, resumeCount);
        Assert.False(coordinator.IsPaused);
    }

    [Fact]
    public void E2E_Reset_ClearsCoordinatorAndTrackerState()
    {
        var config = MakeConfig(max: 10, high: 0.8, low: 0.5);
        var coordinator = new BackpressureCoordinator(config);
        var tracker = new PartitionBackpressureTracker(config);

        // Bring both to paused state
        for (int i = 0; i < 8; i++)
        {
            tracker.RecordBuffered("orders", 0);
            tracker.RecordBuffered("payments", 1);
        }

        coordinator.OnRecordBuffered(8);

        Assert.True(coordinator.IsPaused);
        Assert.True(tracker.ShouldPause("orders", 0));
        Assert.True(tracker.ShouldPause("payments", 1));

        // Reset both
        coordinator.Reset();
        tracker.Reset();

        Assert.False(coordinator.IsPaused);
        Assert.Equal(0, tracker.GetCount("orders", 0));
        Assert.Equal(0, tracker.GetCount("payments", 1));
        Assert.False(tracker.ShouldPause("orders", 0));
        Assert.False(tracker.ShouldPause("payments", 1));
    }

    [Fact]
    public async Task E2E_ConcurrentTrackerUpdates_CountsAreConsistent()
    {
        var config = MakeConfig(max: 1000, high: 0.8, low: 0.5);
        var tracker = new PartitionBackpressureTracker(config);

        const int opsPerTask = 100;
        const int taskCount = 10;

        // Each task buffers opsPerTask records then processes them all
        var tasks = Enumerable.Range(0, taskCount)
            .Select(t => Task.Run(() =>
            {
                for (int i = 0; i < opsPerTask; i++)
                    tracker.RecordBuffered("concurrent", t);

                for (int i = 0; i < opsPerTask; i++)
                    tracker.RecordProcessed("concurrent", t);
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        // All tasks fully processed their records; every partition should be at 0
        for (int t = 0; t < taskCount; t++)
        {
            Assert.Equal(0, tracker.GetCount("concurrent", t));
        }
    }

    [Fact]
    public void E2E_TrackerRemove_AfterReset_DoesNotAffectOtherPartitions()
    {
        var config = MakeConfig(max: 10, high: 0.8, low: 0.5);
        var tracker = new PartitionBackpressureTracker(config);

        for (int i = 0; i < 8; i++)
        {
            tracker.RecordBuffered("topic", 0);
            tracker.RecordBuffered("topic", 1);
        }

        Assert.True(tracker.ShouldPause("topic", 0));
        Assert.True(tracker.ShouldPause("topic", 1));

        // Remove only partition 0 (e.g. partition revocation)
        tracker.Remove("topic", 0);

        Assert.False(tracker.ShouldPause("topic", 0)); // cleared
        Assert.Equal(0, tracker.GetCount("topic", 0));
        Assert.True(tracker.ShouldPause("topic", 1)); // unaffected
    }

    [Fact]
    public void E2E_CoordinatorCycles_MetricsAccumulateCorrectly()
    {
        using var metrics = new StreamsMetrics();
        var config = MakeConfig(max: 10, high: 0.8, low: 0.5);
        var coordinator = new BackpressureCoordinator(config);

        coordinator.OnHighWatermarkReached += () =>
        {
            metrics.RecordHighWatermarkHit();
            metrics.RecordConsumerPause();
        };
        coordinator.OnLowWatermarkReached += () =>
        {
            metrics.RecordLowWatermarkResume();
            metrics.RecordConsumerResume();
        };

        const int cycles = 5;
        for (int i = 0; i < cycles; i++)
        {
            coordinator.OnRecordBuffered(8);  // → paused
            coordinator.OnRecordProcessed(4); // → resumed
        }

        Assert.Equal(cycles, metrics.HighWatermarkHits);
        Assert.Equal(cycles, metrics.LowWatermarkResumes);
        Assert.Equal(cycles, metrics.ConsumerPauses);
        Assert.Equal(cycles, metrics.ConsumerResumes);
    }
}

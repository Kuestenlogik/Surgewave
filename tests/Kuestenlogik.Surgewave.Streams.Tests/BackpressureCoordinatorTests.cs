using Kuestenlogik.Surgewave.Streams.Runtime;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class BackpressureCoordinatorTests
{
    // ------------------------------------------------------------------ helpers

    private static BackpressureConfig MakeConfig(
        int max = 10,
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

    // ------------------------------------------------------------------ BackpressureCoordinator

    [Fact]
    public void Coordinator_NotPaused_Initially()
    {
        var coordinator = new BackpressureCoordinator(MakeConfig());
        Assert.False(coordinator.IsPaused);
    }

    [Fact]
    public void Coordinator_RaisesHighWatermarkEvent_WhenBufferCrossesThreshold()
    {
        var config = MakeConfig(max: 10, high: 0.8); // high watermark = 8
        var coordinator = new BackpressureCoordinator(config);

        int eventCount = 0;
        coordinator.OnHighWatermarkReached += () => eventCount++;

        // Below threshold – no event
        coordinator.OnRecordBuffered(7);
        Assert.Equal(0, eventCount);
        Assert.False(coordinator.IsPaused);

        // At threshold – event fires
        coordinator.OnRecordBuffered(8);
        Assert.Equal(1, eventCount);
        Assert.True(coordinator.IsPaused);
    }

    [Fact]
    public void Coordinator_DoesNotFireHighWatermark_Again_WhileAlreadyPaused()
    {
        var config = MakeConfig(max: 10, high: 0.8);
        var coordinator = new BackpressureCoordinator(config);

        int eventCount = 0;
        coordinator.OnHighWatermarkReached += () => eventCount++;

        coordinator.OnRecordBuffered(8);
        coordinator.OnRecordBuffered(9);
        coordinator.OnRecordBuffered(10);

        Assert.Equal(1, eventCount);
        Assert.True(coordinator.IsPaused);
    }

    [Fact]
    public void Coordinator_RaisesLowWatermarkEvent_WhenBufferDropsBelowThreshold()
    {
        var config = MakeConfig(max: 10, high: 0.8, low: 0.5); // low = 5
        var coordinator = new BackpressureCoordinator(config);

        int resumeCount = 0;
        coordinator.OnLowWatermarkReached += () => resumeCount++;

        // Pause first
        coordinator.OnRecordBuffered(8);
        Assert.True(coordinator.IsPaused);

        // Above low watermark – no resume yet
        coordinator.OnRecordProcessed(6);
        Assert.Equal(0, resumeCount);
        Assert.True(coordinator.IsPaused);

        // At low watermark – resume fires
        coordinator.OnRecordProcessed(5);
        Assert.Equal(1, resumeCount);
        Assert.False(coordinator.IsPaused);
    }

    [Fact]
    public void Coordinator_DoesNotFireLowWatermark_WhenNotPaused()
    {
        var config = MakeConfig(max: 10, low: 0.5);
        var coordinator = new BackpressureCoordinator(config);

        int resumeCount = 0;
        coordinator.OnLowWatermarkReached += () => resumeCount++;

        // Never paused, so dropping below low watermark should be silent
        coordinator.OnRecordProcessed(3);
        coordinator.OnRecordProcessed(0);

        Assert.Equal(0, resumeCount);
        Assert.False(coordinator.IsPaused);
    }

    [Fact]
    public void Coordinator_CanCycleHighLowMultipleTimes()
    {
        var config = MakeConfig(max: 10, high: 0.8, low: 0.5);
        var coordinator = new BackpressureCoordinator(config);

        int highCount = 0;
        int lowCount = 0;
        coordinator.OnHighWatermarkReached += () => highCount++;
        coordinator.OnLowWatermarkReached += () => lowCount++;

        // First cycle
        coordinator.OnRecordBuffered(8);
        Assert.Equal(1, highCount);
        coordinator.OnRecordProcessed(5);
        Assert.Equal(1, lowCount);

        // Second cycle
        coordinator.OnRecordBuffered(8);
        Assert.Equal(2, highCount);
        coordinator.OnRecordProcessed(5);
        Assert.Equal(2, lowCount);
    }

    [Fact]
    public void Coordinator_DoesNotPause_WhenPauseConsumerDisabled()
    {
        var config = MakeConfig(max: 10, high: 0.8, pause: false);
        var coordinator = new BackpressureCoordinator(config);

        int eventCount = 0;
        coordinator.OnHighWatermarkReached += () => eventCount++;

        coordinator.OnRecordBuffered(10);

        Assert.Equal(0, eventCount);
        Assert.False(coordinator.IsPaused);
    }

    [Fact]
    public void Coordinator_Reset_ClearsPausedState()
    {
        var config = MakeConfig(max: 10, high: 0.8);
        var coordinator = new BackpressureCoordinator(config);

        coordinator.OnRecordBuffered(8);
        Assert.True(coordinator.IsPaused);

        coordinator.Reset();
        Assert.False(coordinator.IsPaused);
    }

    // ------------------------------------------------------------------ PartitionBackpressureTracker

    [Fact]
    public void Tracker_ShouldPause_WhenPartitionReachesHighWatermark()
    {
        var config = MakeConfig(max: 10, high: 0.8); // high = 8
        var tracker = new PartitionBackpressureTracker(config);

        for (int i = 0; i < 7; i++)
            tracker.RecordBuffered("topic-a", 0);

        Assert.False(tracker.ShouldPause("topic-a", 0));

        tracker.RecordBuffered("topic-a", 0); // 8th record
        Assert.True(tracker.ShouldPause("topic-a", 0));
    }

    [Fact]
    public void Tracker_ShouldResume_WhenPartitionDropsBelowLowWatermark()
    {
        var config = MakeConfig(max: 10, high: 0.8, low: 0.5); // low = 5
        var tracker = new PartitionBackpressureTracker(config);

        for (int i = 0; i < 8; i++)
            tracker.RecordBuffered("topic-a", 0);

        Assert.True(tracker.ShouldPause("topic-a", 0));

        for (int i = 0; i < 3; i++)
            tracker.RecordProcessed("topic-a", 0); // 5 remaining

        Assert.True(tracker.ShouldResume("topic-a", 0));
    }

    [Fact]
    public void Tracker_PartitionsAreTrackedIndependently()
    {
        var config = MakeConfig(max: 10, high: 0.8);
        var tracker = new PartitionBackpressureTracker(config);

        for (int i = 0; i < 8; i++)
            tracker.RecordBuffered("topic-a", 0);

        for (int i = 0; i < 3; i++)
            tracker.RecordBuffered("topic-a", 1);

        Assert.True(tracker.ShouldPause("topic-a", 0));
        Assert.False(tracker.ShouldPause("topic-a", 1));
    }

    [Fact]
    public void Tracker_MultipleTopicsTrackedSimultaneously()
    {
        var config = MakeConfig(max: 10, high: 0.8);
        var tracker = new PartitionBackpressureTracker(config);

        for (int i = 0; i < 8; i++)
            tracker.RecordBuffered("orders", 0);

        for (int i = 0; i < 8; i++)
            tracker.RecordBuffered("payments", 0);

        for (int i = 0; i < 2; i++)
            tracker.RecordBuffered("audit", 0);

        Assert.True(tracker.ShouldPause("orders", 0));
        Assert.True(tracker.ShouldPause("payments", 0));
        Assert.False(tracker.ShouldPause("audit", 0));
    }

    [Fact]
    public void Tracker_CountNeverGoesNegative()
    {
        var config = MakeConfig(max: 10);
        var tracker = new PartitionBackpressureTracker(config);

        tracker.RecordProcessed("topic-a", 0);
        tracker.RecordProcessed("topic-a", 0);

        Assert.Equal(0, tracker.GetCount("topic-a", 0));
    }

    [Fact]
    public void Tracker_Remove_ClearsPartitionState()
    {
        var config = MakeConfig(max: 10, high: 0.8);
        var tracker = new PartitionBackpressureTracker(config);

        for (int i = 0; i < 8; i++)
            tracker.RecordBuffered("topic-a", 0);

        Assert.True(tracker.ShouldPause("topic-a", 0));

        tracker.Remove("topic-a", 0);

        Assert.False(tracker.ShouldPause("topic-a", 0));
        Assert.Equal(0, tracker.GetCount("topic-a", 0));
    }

    [Fact]
    public void Tracker_ShouldResume_TrueForUnknownPartition()
    {
        var config = MakeConfig(max: 10);
        var tracker = new PartitionBackpressureTracker(config);

        // A partition we've never seen should be considered safe to resume
        Assert.True(tracker.ShouldResume("topic-a", 99));
    }

    // ------------------------------------------------------------------ StreamsMetrics backpressure counters

    [Fact]
    public void Metrics_BackpressureCounters_StartAtZero()
    {
        using var metrics = new StreamsMetrics();
        Assert.Equal(0, metrics.HighWatermarkHits);
        Assert.Equal(0, metrics.LowWatermarkResumes);
        Assert.Equal(0, metrics.ConsumerPauses);
        Assert.Equal(0, metrics.ConsumerResumes);
    }

    [Fact]
    public void Metrics_RecordHighWatermarkHit_Increments()
    {
        using var metrics = new StreamsMetrics();
        metrics.RecordHighWatermarkHit();
        metrics.RecordHighWatermarkHit();
        Assert.Equal(2, metrics.HighWatermarkHits);
    }

    [Fact]
    public void Metrics_RecordLowWatermarkResume_Increments()
    {
        using var metrics = new StreamsMetrics();
        metrics.RecordLowWatermarkResume();
        Assert.Equal(1, metrics.LowWatermarkResumes);
    }

    [Fact]
    public void Metrics_RecordConsumerPauseAndResume_Increments()
    {
        using var metrics = new StreamsMetrics();
        metrics.RecordConsumerPause();
        metrics.RecordConsumerPause();
        metrics.RecordConsumerResume();
        Assert.Equal(2, metrics.ConsumerPauses);
        Assert.Equal(1, metrics.ConsumerResumes);
    }

    // ------------------------------------------------------------------ Coordinator + Metrics integration

    [Fact]
    public void Coordinator_FiresEvents_AndMetricsRecordThem()
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

        coordinator.OnRecordBuffered(8);
        Assert.Equal(1, metrics.HighWatermarkHits);
        Assert.Equal(1, metrics.ConsumerPauses);

        coordinator.OnRecordProcessed(5);
        Assert.Equal(1, metrics.LowWatermarkResumes);
        Assert.Equal(1, metrics.ConsumerResumes);
    }
}

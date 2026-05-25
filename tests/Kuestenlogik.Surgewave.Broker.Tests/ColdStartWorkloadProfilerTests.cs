using Kuestenlogik.Surgewave.Broker.AutoTuning;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

public sealed class ColdStartWorkloadProfilerTests
{
    [Fact]
    public void Newly_Constructed_Profiler_Is_Not_Complete()
    {
        var time = new TestTimeProvider(DateTimeOffset.UtcNow);
        var profiler = new ColdStartWorkloadProfiler(TimeSpan.FromHours(24), time);

        Assert.False(profiler.IsComplete);
        Assert.Equal(TimeSpan.FromHours(24), profiler.ObservationWindow);
    }

    [Fact]
    public void IsComplete_Flips_After_Observation_Window_Elapses()
    {
        var time = new TestTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var profiler = new ColdStartWorkloadProfiler(TimeSpan.FromHours(24), time);

        time.Advance(TimeSpan.FromHours(23));
        Assert.False(profiler.IsComplete);

        time.Advance(TimeSpan.FromHours(2));
        Assert.True(profiler.IsComplete);
    }

    [Fact]
    public void RecordProduce_Accumulates_Totals_And_Per_Topic()
    {
        var profiler = new ColdStartWorkloadProfiler();
        profiler.RecordProduce("orders", recordCount: 5, byteCount: 500);
        profiler.RecordProduce("orders", recordCount: 3, byteCount: 300);
        profiler.RecordProduce("metrics", recordCount: 10, byteCount: 100);

        var profile = profiler.BuildProfile();
        Assert.Equal(18, profile.TotalRecords);
        Assert.Equal(900, profile.TotalBytes);
        Assert.Equal(2, profile.TopicCardinality);

        var orders = Assert.Single(profile.Topics, t => t.Topic == "orders");
        Assert.Equal(8, orders.TotalRecords);
        Assert.Equal(800, orders.TotalBytes);
        var metrics = Assert.Single(profile.Topics, t => t.Topic == "metrics");
        Assert.Equal(10, metrics.TotalRecords);
        Assert.Equal(100, metrics.TotalBytes);
    }

    [Fact]
    public void RecordProduce_Ignores_Negative_And_Empty_Inputs()
    {
        var profiler = new ColdStartWorkloadProfiler();
        profiler.RecordProduce(string.Empty, 1, 1);
        profiler.RecordProduce("t", 0, 100);
        profiler.RecordProduce("t", -5, 100);
        profiler.RecordProduce("t", 1, -10);

        var profile = profiler.BuildProfile();
        Assert.Equal(0, profile.TotalRecords);
        Assert.Equal(0, profile.TopicCardinality);
    }

    [Fact]
    public void Peak_Per_Second_Tracks_The_Heaviest_Wall_Second()
    {
        var time = new TestTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var profiler = new ColdStartWorkloadProfiler(TimeSpan.FromHours(24), time);

        // Second 0: 100 records / 1000 bytes
        profiler.RecordProduce("t", 100, 1000);
        time.Advance(TimeSpan.FromSeconds(1));
        // Second 1: 50 records / 500 bytes
        profiler.RecordProduce("t", 50, 500);
        time.Advance(TimeSpan.FromSeconds(1));
        // Second 2: 200 records / 2000 bytes — new peak
        profiler.RecordProduce("t", 200, 2000);
        time.Advance(TimeSpan.FromSeconds(1));
        // Trigger roll-over of second 2's totals into the peak comparator.
        profiler.RecordProduce("t", 1, 1);

        var profile = profiler.BuildProfile();
        Assert.Equal(200, profile.PeakRecordsPerSecond);
        Assert.Equal(2000, profile.PeakBytesPerSecond);
    }

    [Fact]
    public void RecordReplicationLag_Tracks_Average_And_Max()
    {
        var profiler = new ColdStartWorkloadProfiler();
        profiler.RecordReplicationLag(100);
        profiler.RecordReplicationLag(500);
        profiler.RecordReplicationLag(300);

        var profile = profiler.BuildProfile();
        Assert.Equal(3, profile.ReplicationLagSamples);
        Assert.Equal(300, profile.AverageReplicationLagMs); // (100 + 500 + 300) / 3
        Assert.Equal(500, profile.MaxReplicationLagMs);
    }

    [Fact]
    public void RecordReplicationLag_Ignores_Negative_Samples()
    {
        var profiler = new ColdStartWorkloadProfiler();
        profiler.RecordReplicationLag(-1);
        profiler.RecordReplicationLag(-99);

        var profile = profiler.BuildProfile();
        Assert.Equal(0, profile.ReplicationLagSamples);
        Assert.Equal(0, profile.AverageReplicationLagMs);
        Assert.Equal(0, profile.MaxReplicationLagMs);
    }

    [Fact]
    public void Concurrent_Observers_Are_Loss_Free()
    {
        var profiler = new ColdStartWorkloadProfiler();
        Parallel.For(0, 8, _ =>
        {
            for (var i = 0; i < 500; i++)
            {
                profiler.RecordProduce("t", 1, 10);
            }
        });

        var profile = profiler.BuildProfile();
        Assert.Equal(4000, profile.TotalRecords);
        Assert.Equal(40_000, profile.TotalBytes);
    }

    [Fact]
    public void Topics_Are_Sorted_By_Records_Descending()
    {
        var profiler = new ColdStartWorkloadProfiler();
        profiler.RecordProduce("low", 10, 100);
        profiler.RecordProduce("high", 1000, 10_000);
        profiler.RecordProduce("medium", 100, 1000);

        var profile = profiler.BuildProfile();
        Assert.Equal("high", profile.Topics[0].Topic);
        Assert.Equal("medium", profile.Topics[1].Topic);
        Assert.Equal("low", profile.Topics[2].Topic);
    }

    private sealed class TestTimeProvider : TimeProvider
    {
        private DateTimeOffset _now;
        public TestTimeProvider(DateTimeOffset start) => _now = start;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan delta) => _now = _now.Add(delta);
    }
}

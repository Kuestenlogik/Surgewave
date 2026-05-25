namespace Kuestenlogik.Surgewave.Connect.Tests.Pipelines;

using Kuestenlogik.Surgewave.Connect.Pipelines;

public class PipelineMetricsCollectorTests
{
    [Fact]
    public void RecordProcessed_IncreasesCount()
    {
        var collector = new PipelineMetricsCollector();

        collector.RecordProcessed("p1", "n1", 5.0);
        collector.RecordProcessed("p1", "n1", 10.0);

        var metrics = collector.GetMetrics("p1");
        Assert.NotNull(metrics);
        Assert.Equal(2, metrics!.TotalRecordsProcessed);
    }

    [Fact]
    public void RecordError_IncreasesErrorCount()
    {
        var collector = new PipelineMetricsCollector();

        collector.RecordError("p1", "n1");
        collector.RecordError("p1", "n1");

        var metrics = collector.GetMetrics("p1");
        Assert.NotNull(metrics);
        Assert.Equal(2, metrics!.TotalErrors);
    }

    [Fact]
    public void ThroughputCalculation_ReturnsPositiveRate()
    {
        var collector = new PipelineMetricsCollector();

        for (var i = 0; i < 100; i++)
            collector.RecordProcessed("p1", "n1", 1.0);

        var metrics = collector.GetMetrics("p1");
        Assert.NotNull(metrics);
        Assert.True(metrics!.RecordsPerSecond > 0);
    }

    [Fact]
    public void Reset_ClearsMetrics()
    {
        var collector = new PipelineMetricsCollector();

        collector.RecordProcessed("p1", "n1", 5.0);
        collector.Reset("p1");

        var metrics = collector.GetMetrics("p1");
        Assert.Null(metrics);
    }

    [Fact]
    public void ConcurrentAccess_NoErrors()
    {
        var collector = new PipelineMetricsCollector();

        Parallel.For(0, 1000, i =>
        {
            collector.RecordProcessed("p1", $"n{i % 5}", i * 0.1);
        });

        var metrics = collector.GetMetrics("p1");
        Assert.NotNull(metrics);
        Assert.Equal(1000, metrics!.TotalRecordsProcessed);
    }

    [Fact]
    public void LatencyPercentiles_CorrectValues()
    {
        var collector = new PipelineMetricsCollector();

        for (var i = 1; i <= 100; i++)
            collector.RecordProcessed("p1", "n1", i);

        var nodeMetrics = collector.GetNodeMetrics("p1", "n1");
        Assert.NotNull(nodeMetrics);
        Assert.True(nodeMetrics!.P50LatencyMs >= 49 && nodeMetrics.P50LatencyMs <= 51);
        Assert.True(nodeMetrics.P95LatencyMs >= 94 && nodeMetrics.P95LatencyMs <= 96);
        Assert.True(nodeMetrics.P99LatencyMs >= 98 && nodeMetrics.P99LatencyMs <= 100);
    }
}

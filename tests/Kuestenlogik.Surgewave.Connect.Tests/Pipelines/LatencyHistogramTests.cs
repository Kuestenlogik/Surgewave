namespace Kuestenlogik.Surgewave.Connect.Tests.Pipelines;

using Kuestenlogik.Surgewave.Connect.Pipelines;

public class LatencyHistogramTests
{
    [Fact]
    public void Percentile_CorrectCalculation()
    {
        var histogram = new LatencyHistogram();

        for (var i = 1; i <= 100; i++)
            histogram.Record(i);

        var p50 = histogram.GetPercentile(50);
        Assert.True(p50 >= 49 && p50 <= 51);

        var p99 = histogram.GetPercentile(99);
        Assert.True(p99 >= 98 && p99 <= 100);
    }

    [Fact]
    public void EmptyHistogram_ReturnsZero()
    {
        var histogram = new LatencyHistogram();

        Assert.Equal(0, histogram.GetPercentile(50));
        Assert.Equal(0, histogram.Average);
    }

    [Fact]
    public void RingBufferOverflow_KeepsLatestValues()
    {
        var histogram = new LatencyHistogram();

        // Fill beyond the 10,000 capacity
        for (var i = 0; i < 15_000; i++)
            histogram.Record(i);

        Assert.Equal(10_000, histogram.Count);

        // The average should reflect the latest values (5000..14999)
        var avg = histogram.Average;
        Assert.True(avg > 5000);
    }

    [Fact]
    public void Reset_ClearsAllData()
    {
        var histogram = new LatencyHistogram();

        for (var i = 0; i < 100; i++)
            histogram.Record(i);

        histogram.Reset();

        Assert.Equal(0, histogram.Count);
        Assert.Equal(0, histogram.GetPercentile(50));
    }
}

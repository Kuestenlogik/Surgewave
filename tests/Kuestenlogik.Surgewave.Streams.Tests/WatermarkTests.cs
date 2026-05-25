using Kuestenlogik.Surgewave.Streams.EventTime;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class WatermarkTests
{
    [Fact]
    public void Watermark_PropagationThroughContext()
    {
        var config = new StreamsConfig { ApplicationId = "test", BootstrapServers = "localhost:9092" };
        var context = new ProcessorContext(config, new StreamsMetrics(),
            NullLoggerFactory.Instance.CreateLogger("test"));

        Assert.True(context.CurrentWatermark.IsNone);

        context.UpdateWatermark(new Watermark(1000));
        Assert.Equal(1000, context.CurrentWatermark.Timestamp);

        // Watermark should only advance
        context.UpdateWatermark(new Watermark(500));
        Assert.Equal(1000, context.CurrentWatermark.Timestamp);

        context.UpdateWatermark(new Watermark(2000));
        Assert.Equal(2000, context.CurrentWatermark.Timestamp);
    }

    [Fact]
    public void BoundedOutOfOrderness_DropsLateEvents()
    {
        var strategy = WatermarkStrategy<int>
            .ForBoundedOutOfOrderness(TimeSpan.FromSeconds(5));

        var generator = strategy.CreateWatermarkGenerator();

        generator.OnEvent(1, 10000);
        var wm1 = generator.GetCurrentWatermark();
        Assert.Equal(10000 - 5001, wm1.Timestamp);

        generator.OnEvent(2, 20000);
        var wm2 = generator.GetCurrentWatermark();
        Assert.Equal(20000 - 5001, wm2.Timestamp);
    }

    [Fact]
    public void MonotonousTimestamps_StrictlyAscending()
    {
        var strategy = WatermarkStrategy<string>.ForMonotonousTimestamps();

        var generator = strategy.CreateWatermarkGenerator();

        generator.OnEvent("a", 1000);
        Assert.Equal(999, generator.GetCurrentWatermark().Timestamp);

        generator.OnEvent("b", 2000);
        Assert.Equal(1999, generator.GetCurrentWatermark().Timestamp);
    }

    [Fact]
    public void Context_ExposesWatermark()
    {
        var config = new StreamsConfig { ApplicationId = "test", BootstrapServers = "localhost:9092" };
        var context = new ProcessorContext(config, new StreamsMetrics(),
            NullLoggerFactory.Instance.CreateLogger("test"));

        Assert.True(context.CurrentWatermark.IsNone);
        Assert.Equal(long.MinValue, context.CurrentWatermark.Timestamp);

        context.UpdateWatermark(Watermark.FromTimestamp(5000));
        Assert.False(context.CurrentWatermark.IsNone);
        Assert.Equal(5000, context.CurrentWatermark.Timestamp);
    }

    [Fact]
    public void EventTimeStream_AssignsWatermarks()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");

        var strategy = WatermarkStrategy<int>
            .ForBoundedOutOfOrderness(TimeSpan.FromSeconds(5))
            .WithTimestampAssigner((value, ts) => ts);

        var eventTimeStream = stream.AssignTimestampsAndWatermarks(strategy);

        Assert.NotNull(eventTimeStream);
        Assert.IsAssignableFrom<IEventTimeStream<string, int>>(eventTimeStream);
    }

    [Fact]
    public void IdleSources_WatermarkNone()
    {
        var strategy = WatermarkStrategy<string>
            .ForMonotonousTimestamps()
            .WithIdleness(TimeSpan.FromSeconds(30));

        Assert.Equal(TimeSpan.FromSeconds(30), strategy.IdleTimeout);

        var generator = strategy.CreateWatermarkGenerator();
        Assert.True(generator.GetCurrentWatermark().IsNone);
    }
}

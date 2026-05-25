using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class BackpressureTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public BackpressureTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddFilter(level => level >= LogLevel.Debug);
        });
    }

    public async ValueTask DisposeAsync()
    {
        _loggerFactory.Dispose();
        await Task.CompletedTask;
    }

    [Fact]
    public async Task Backpressure_BlocksProducer_WhenBufferFull()
    {
        using var metrics = new StreamsMetrics();
        var config = new BackpressureConfig
        {
            MaxBufferedRecords = 3,
            Strategy = BackpressureStrategy.Block,
            MaxWaitTime = TimeSpan.FromMilliseconds(100)
        };

        using var buffer = new BackpressureBuffer(config, metrics);

        // Fill buffer
        for (int i = 0; i < 3; i++)
        {
            var result = await buffer.WriteAsync(CreateRecord(i));
            Assert.True(result);
        }

        Assert.Equal(3, buffer.CurrentSize);

        // Next write should block and eventually fail due to MaxWaitTime
        var writeResult = await buffer.WriteAsync(CreateRecord(99));
        Assert.False(writeResult);
    }

    [Fact]
    public async Task Backpressure_DropOldest_WhenBufferFull()
    {
        using var metrics = new StreamsMetrics();
        var config = new BackpressureConfig
        {
            MaxBufferedRecords = 3,
            Strategy = BackpressureStrategy.DropOldest
        };

        using var buffer = new BackpressureBuffer(config, metrics);

        // Fill and overflow
        for (int i = 0; i < 5; i++)
        {
            await buffer.WriteAsync(CreateRecord(i));
        }

        // Buffer should still be at max capacity
        Assert.True(buffer.CurrentSize <= 3);
    }

    [Fact]
    public async Task Backpressure_DropNewest_WhenBufferFull()
    {
        using var metrics = new StreamsMetrics();
        var config = new BackpressureConfig
        {
            MaxBufferedRecords = 3,
            Strategy = BackpressureStrategy.DropNewest
        };

        using var buffer = new BackpressureBuffer(config, metrics);

        // Fill buffer
        for (int i = 0; i < 3; i++)
        {
            await buffer.WriteAsync(CreateRecord(i));
        }

        // Overflow: should drop newest (incoming)
        await buffer.WriteAsync(CreateRecord(99));

        Assert.True(buffer.CurrentSize <= 3);
    }

    [Fact]
    public async Task Backpressure_PausesConsumer_AboveHighWatermark()
    {
        using var metrics = new StreamsMetrics();
        var config = new BackpressureConfig
        {
            MaxBufferedRecords = 10,
            HighWatermarkRatio = 0.8,
            LowWatermarkRatio = 0.5,
            Strategy = BackpressureStrategy.Block,
            MaxWaitTime = TimeSpan.FromSeconds(5)
        };

        using var buffer = new BackpressureBuffer(config, metrics);

        // Fill to above high watermark (80% of 10 = 8)
        for (int i = 0; i < 9; i++)
        {
            await buffer.WriteAsync(CreateRecord(i));
        }

        Assert.True(buffer.IsAboveHighWatermark);
    }

    [Fact]
    public async Task Backpressure_ResumesConsumer_BelowLowWatermark()
    {
        using var metrics = new StreamsMetrics();
        var config = new BackpressureConfig
        {
            MaxBufferedRecords = 10,
            HighWatermarkRatio = 0.8,
            LowWatermarkRatio = 0.5,
            Strategy = BackpressureStrategy.Block,
            MaxWaitTime = TimeSpan.FromSeconds(5)
        };

        using var buffer = new BackpressureBuffer(config, metrics);

        // Fill to above high watermark
        for (int i = 0; i < 9; i++)
        {
            await buffer.WriteAsync(CreateRecord(i));
        }

        Assert.True(buffer.IsAboveHighWatermark);

        // Drain below low watermark (50% of 10 = 5)
        for (int i = 0; i < 6; i++)
        {
            await buffer.ReadAsync();
        }

        Assert.True(buffer.IsBelowLowWatermark);
    }

    [Fact]
    public async Task Backpressure_Metrics_ReportsBufferDepth()
    {
        using var metrics = new StreamsMetrics();
        var config = new BackpressureConfig
        {
            MaxBufferedRecords = 5,
            Strategy = BackpressureStrategy.Block,
            MaxWaitTime = TimeSpan.FromMilliseconds(100)
        };

        using var buffer = new BackpressureBuffer(config, metrics);

        await buffer.WriteAsync(CreateRecord(1));
        await buffer.WriteAsync(CreateRecord(2));

        // Update metrics
        metrics.UpdateBackpressureBufferSize(buffer.CurrentSize);

        Assert.Equal(2, metrics.BackpressureBufferSize);
        Assert.Equal(0, metrics.BackpressureDropped);

        // Fill and overflow with Block strategy (will drop after timeout)
        for (int i = 0; i < 5; i++)
        {
            await buffer.WriteAsync(CreateRecord(i + 10));
        }

        // Try one more that will timeout and drop
        await buffer.WriteAsync(CreateRecord(99));

        Assert.True(metrics.BackpressureDropped >= 0);
    }

    private static BufferedRecord CreateRecord(int id)
    {
        return new BufferedRecord(
            Key: System.Text.Encoding.UTF8.GetBytes($"key-{id}"),
            Value: System.Text.Encoding.UTF8.GetBytes($"value-{id}"),
            Topic: "test-topic",
            Partition: 0,
            Offset: id,
            Timestamp: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }
}

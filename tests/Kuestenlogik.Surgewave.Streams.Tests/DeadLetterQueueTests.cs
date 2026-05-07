using System.Text.Json;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Dlq;
using Kuestenlogik.Surgewave.Streams.ExceptionHandling;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class DeadLetterQueueTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public DeadLetterQueueTests(ITestOutputHelper output)
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
    public void DeadLetterQueueConfig_Defaults()
    {
        var config = DeadLetterQueueConfig.Disabled;

        Assert.False(config.Enabled);
        Assert.Equal(".DLQ", config.TopicSuffix);
        Assert.Equal(0, config.MaxRetries);
        Assert.True(config.IncludeStackTrace);
        Assert.True(config.IncludeHeaders);
    }

    [Fact]
    public void DeadLetterQueueConfig_GetDlqTopicName()
    {
        var config = new DeadLetterQueueConfig { Enabled = true, TopicSuffix = ".DLQ" };

        Assert.Equal("orders.DLQ", config.GetDlqTopicName("orders"));
        Assert.Equal("user-events.DLQ", config.GetDlqTopicName("user-events"));

        var customConfig = new DeadLetterQueueConfig { Enabled = true, TopicSuffix = "-dead" };
        Assert.Equal("orders-dead", customConfig.GetDlqTopicName("orders"));
    }

    [Fact]
    public void DeadLetterRecord_CapturesErrorMetadata()
    {
        var record = new DeadLetterRecord
        {
            OriginalTopic = "input",
            OriginalPartition = 2,
            OriginalOffset = 42,
            Key = [1, 2, 3],
            Value = [4, 5, 6],
            Timestamp = 1000,
            ApplicationId = "test-app",
            ExceptionType = "System.InvalidOperationException",
            ExceptionMessage = "Test error",
            StackTrace = "at Test.Method()",
            RetryCount = 1,
            FailedAt = DateTimeOffset.UtcNow
        };

        Assert.Equal("input", record.OriginalTopic);
        Assert.Equal(2, record.OriginalPartition);
        Assert.Equal(42, record.OriginalOffset);
        Assert.Equal("System.InvalidOperationException", record.ExceptionType);
        Assert.Equal("Test error", record.ExceptionMessage);
        Assert.Equal(1, record.RetryCount);

        // Ensure it serializes to JSON
        var json = JsonSerializer.Serialize(record);
        Assert.Contains("input", json);
        Assert.Contains("Test error", json);
    }

    [Fact]
    public void DeadLetterQueueConfig_Enabled_WithDefaults()
    {
        var config = new DeadLetterQueueConfig { Enabled = true };

        Assert.True(config.Enabled);
        Assert.Equal(".DLQ", config.TopicSuffix);
        Assert.Equal(0, config.MaxRetries);
        Assert.True(config.IncludeStackTrace);
    }

    [Fact]
    public void StreamsConfig_DeadLetterQueue_DefaultDisabled()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "dlq-default",
            BootstrapServers = "localhost:9092"
        };

        Assert.False(config.DeadLetterQueue.Enabled);
    }

    [Fact]
    public void StreamsConfig_DeadLetterQueue_CanBeEnabled()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "dlq-task",
            BootstrapServers = "localhost:9092",
            DeadLetterQueue = new DeadLetterQueueConfig
            {
                Enabled = true,
                TopicSuffix = ".DLQ",
                MaxRetries = 3,
                IncludeStackTrace = false
            }
        };

        Assert.True(config.DeadLetterQueue.Enabled);
        Assert.Equal(".DLQ", config.DeadLetterQueue.TopicSuffix);
        Assert.Equal(3, config.DeadLetterQueue.MaxRetries);
        Assert.False(config.DeadLetterQueue.IncludeStackTrace);

        // Builder should accept the config
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").ForEach((k, v) => { });
        var topology = builder.Build();
        var app = new StreamsApplication(config, topology, _loggerFactory);

        Assert.NotNull(app.Metrics);
        Assert.Equal(0, app.Metrics.DlqMessages);
    }
}

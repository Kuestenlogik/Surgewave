using Kuestenlogik.Surgewave.Core.Dlq;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for DLQ (Dead Letter Queue) components: DlqConfig, DlqRecord, DlqRecordSerializer, DlqRouter, DlqMetrics.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DlqTests
{
    #region DlqConfig Tests

    [Fact]
    public void DlqConfig_DefaultValues_AreCorrect()
    {
        var config = new DlqConfig();

        Assert.True(config.Enabled);
        Assert.Equal(3, config.MaxRetries);
        Assert.Equal(1000, config.RetryBackoffMs);
        Assert.Equal(".DLQ", config.TopicSuffix);
        Assert.True(config.IncludeStackTrace);
        Assert.Equal(1, config.DlqPartitionCount);
        Assert.Equal(604800000L, config.RetentionMs); // 7 days
    }

    [Theory]
    [InlineData("orders", ".DLQ", "orders.DLQ")]
    [InlineData("events", ".DLQ", "events.DLQ")]
    [InlineData("my-topic", "-dlq", "my-topic-dlq")]
    [InlineData("a", ".dead-letter", "a.dead-letter")]
    public void DlqConfig_GetDlqTopicName_FormatsCorrectly(string original, string suffix, string expected)
    {
        var config = new DlqConfig { TopicSuffix = suffix };
        Assert.Equal(expected, config.GetDlqTopicName(original));
    }

    [Fact]
    public void DlqConfig_CustomValues_SetCorrectly()
    {
        var config = new DlqConfig
        {
            Enabled = false,
            MaxRetries = 5,
            RetryBackoffMs = 2000,
            TopicSuffix = ".dead",
            IncludeStackTrace = false,
            DlqPartitionCount = 3,
            RetentionMs = 86400000
        };

        Assert.False(config.Enabled);
        Assert.Equal(5, config.MaxRetries);
        Assert.Equal(2000, config.RetryBackoffMs);
        Assert.Equal(".dead", config.TopicSuffix);
        Assert.False(config.IncludeStackTrace);
        Assert.Equal(3, config.DlqPartitionCount);
        Assert.Equal(86400000L, config.RetentionMs);
    }

    #endregion

    #region DlqRecord Tests

    [Fact]
    public void DlqRecord_RequiredProperties_SetCorrectly()
    {
        var now = DateTimeOffset.UtcNow;
        var record = new DlqRecord
        {
            OriginalTopic = "orders",
            OriginalPartition = 2,
            OriginalOffset = 42,
            OriginalKey = [1, 2, 3],
            OriginalValue = [4, 5, 6],
            OriginalTimestamp = now,
            ExceptionType = "InvalidOperationException",
            ExceptionMessage = "Something went wrong",
            StackTrace = "at Method()",
            SourceName = "my-connector",
            SourceType = "connect-sink",
            TaskId = "task-0",
            AttemptCount = 3,
            FailedAt = now
        };

        Assert.Equal("orders", record.OriginalTopic);
        Assert.Equal(2, record.OriginalPartition);
        Assert.Equal(42, record.OriginalOffset);
        Assert.Equal([1, 2, 3], record.OriginalKey);
        Assert.Equal([4, 5, 6], record.OriginalValue);
        Assert.Equal(now, record.OriginalTimestamp);
        Assert.Equal("InvalidOperationException", record.ExceptionType);
        Assert.Equal("Something went wrong", record.ExceptionMessage);
        Assert.Equal("at Method()", record.StackTrace);
        Assert.Equal("my-connector", record.SourceName);
        Assert.Equal("connect-sink", record.SourceType);
        Assert.Equal("task-0", record.TaskId);
        Assert.Equal(3, record.AttemptCount);
        Assert.Equal(now, record.FailedAt);
    }

    [Fact]
    public void DlqRecord_OptionalProperties_CanBeNull()
    {
        var record = new DlqRecord
        {
            OriginalTopic = "t",
            OriginalPartition = 0,
            OriginalOffset = 0,
            OriginalValue = [],
            ExceptionType = "Ex",
            ExceptionMessage = "msg",
            SourceName = "src",
            SourceType = "consumer"
        };

        Assert.Null(record.OriginalKey);
        Assert.Null(record.OriginalHeaders);
        Assert.Null(record.StackTrace);
        Assert.Null(record.TaskId);
        Assert.Null(record.AdditionalContext);
    }

    [Fact]
    public void DlqRecord_WithHeaders_PreservesHeaders()
    {
        var headers = new Dictionary<string, byte[]>
        {
            ["header1"] = [10, 20],
            ["header2"] = [30, 40]
        };

        var record = new DlqRecord
        {
            OriginalTopic = "t",
            OriginalPartition = 0,
            OriginalOffset = 0,
            OriginalValue = [1],
            OriginalHeaders = headers,
            ExceptionType = "Ex",
            ExceptionMessage = "msg",
            SourceName = "src",
            SourceType = "consumer"
        };

        Assert.Equal(2, record.OriginalHeaders!.Count);
        Assert.Equal([10, 20], record.OriginalHeaders["header1"]);
    }

    [Fact]
    public void DlqRecord_WithAdditionalContext_PreservesContext()
    {
        var context = new Dictionary<string, string>
        {
            ["region"] = "us-west-2",
            ["cluster"] = "production"
        };

        var record = new DlqRecord
        {
            OriginalTopic = "t",
            OriginalPartition = 0,
            OriginalOffset = 0,
            OriginalValue = [1],
            ExceptionType = "Ex",
            ExceptionMessage = "msg",
            SourceName = "src",
            SourceType = "consumer",
            AdditionalContext = context
        };

        Assert.Equal(2, record.AdditionalContext!.Count);
        Assert.Equal("us-west-2", record.AdditionalContext["region"]);
    }

    #endregion

    #region DlqRecordSerializer Tests

    [Fact]
    public void DlqRecordSerializer_RoundTrip_PreservesAllFields()
    {
        var now = DateTimeOffset.UtcNow;
        var record = new DlqRecord
        {
            OriginalTopic = "orders",
            OriginalPartition = 3,
            OriginalOffset = 100,
            OriginalKey = [1, 2, 3],
            OriginalValue = [4, 5, 6, 7],
            OriginalTimestamp = now,
            OriginalHeaders = new Dictionary<string, byte[]>
            {
                ["h1"] = [10, 20]
            },
            ExceptionType = "NullReferenceException",
            ExceptionMessage = "Object reference not set",
            StackTrace = "at MyClass.MyMethod()",
            SourceName = "jdbc-sink",
            SourceType = "connect-sink",
            TaskId = "task-1",
            AttemptCount = 5,
            FailedAt = now,
            AdditionalContext = new Dictionary<string, string>
            {
                ["env"] = "production"
            }
        };

        var bytes = DlqRecordSerializer.Serialize(record);
        var deserialized = DlqRecordSerializer.Deserialize(bytes);

        Assert.Equal(record.OriginalTopic, deserialized.OriginalTopic);
        Assert.Equal(record.OriginalPartition, deserialized.OriginalPartition);
        Assert.Equal(record.OriginalOffset, deserialized.OriginalOffset);
        Assert.Equal(record.OriginalKey, deserialized.OriginalKey);
        Assert.Equal(record.OriginalValue, deserialized.OriginalValue);
        Assert.Equal(record.ExceptionType, deserialized.ExceptionType);
        Assert.Equal(record.ExceptionMessage, deserialized.ExceptionMessage);
        Assert.Equal(record.StackTrace, deserialized.StackTrace);
        Assert.Equal(record.SourceName, deserialized.SourceName);
        Assert.Equal(record.SourceType, deserialized.SourceType);
        Assert.Equal(record.TaskId, deserialized.TaskId);
        Assert.Equal(record.AttemptCount, deserialized.AttemptCount);
    }

    [Fact]
    public void DlqRecordSerializer_NullOptionalFields_RoundTrip()
    {
        var record = new DlqRecord
        {
            OriginalTopic = "t",
            OriginalPartition = 0,
            OriginalOffset = 0,
            OriginalValue = [42],
            ExceptionType = "Ex",
            ExceptionMessage = "msg",
            SourceName = "src",
            SourceType = "consumer"
        };

        var bytes = DlqRecordSerializer.Serialize(record);
        var deserialized = DlqRecordSerializer.Deserialize(bytes);

        Assert.Null(deserialized.OriginalKey);
        Assert.Null(deserialized.StackTrace);
        Assert.Null(deserialized.TaskId);
    }

    [Fact]
    public void DlqRecordSerializer_EmptyValue_RoundTrip()
    {
        var record = new DlqRecord
        {
            OriginalTopic = "t",
            OriginalPartition = 0,
            OriginalOffset = 0,
            OriginalValue = [],
            ExceptionType = "Ex",
            ExceptionMessage = "msg",
            SourceName = "src",
            SourceType = "consumer"
        };

        var bytes = DlqRecordSerializer.Serialize(record);
        var deserialized = DlqRecordSerializer.Deserialize(bytes);

        Assert.Empty(deserialized.OriginalValue);
    }

    #endregion

    #region DlqRouter Tests

    [Fact]
    public async Task DlqRouter_WhenDisabled_ReturnsFalse()
    {
        var config = new DlqConfig { Enabled = false };
        var producer = new FakeDlqProducer();
        var router = new DlqRouter(config, producer);

        var record = CreateTestDlqRecord();
        var result = await router.RouteAsync(record);

        Assert.False(result);
        Assert.Empty(producer.ProducedMessages);
    }

    [Fact]
    public async Task DlqRouter_WhenEnabled_RoutesToDlqTopic()
    {
        var config = new DlqConfig();
        var producer = new FakeDlqProducer();
        var router = new DlqRouter(config, producer);

        var record = CreateTestDlqRecord("orders");
        var result = await router.RouteAsync(record);

        Assert.True(result);
        Assert.Single(producer.ProducedMessages);
        Assert.Equal("orders.DLQ", producer.ProducedMessages[0].Topic);
    }

    [Fact]
    public async Task DlqRouter_RouteBatch_RoutesAllRecords()
    {
        var config = new DlqConfig();
        var producer = new FakeDlqProducer();
        var router = new DlqRouter(config, producer);

        var records = new[]
        {
            CreateTestDlqRecord("topic-a"),
            CreateTestDlqRecord("topic-b"),
            CreateTestDlqRecord("topic-a")
        };

        var count = await router.RouteBatchAsync(records);

        Assert.Equal(3, count);
        Assert.Equal(3, producer.ProducedMessages.Count);
    }

    [Fact]
    public async Task DlqRouter_EnsuresTopicExistsOnce()
    {
        var config = new DlqConfig();
        var producer = new FakeDlqProducer();
        var router = new DlqRouter(config, producer);

        // Route two records to the same original topic
        await router.RouteAsync(CreateTestDlqRecord("orders"));
        await router.RouteAsync(CreateTestDlqRecord("orders"));

        // EnsureTopicExists should only be called once (cached)
        var call = Assert.Single(producer.EnsureTopicCalls);
        Assert.Equal("orders.DLQ", call.Topic);
    }

    [Fact]
    public void DlqRouter_NullConfig_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new DlqRouter(null!, new FakeDlqProducer()));
    }

    [Fact]
    public void DlqRouter_NullProducer_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new DlqRouter(new DlqConfig(), null!));
    }

    #endregion

    #region DlqMetrics Tests

    [Fact]
    public void DlqMetrics_MeterName_IsCorrect()
    {
        Assert.Equal("Kuestenlogik.Surgewave.Dlq", DlqMetrics.MeterName);
    }

    [Fact]
    public void DlqMetrics_RecordMethods_DoNotThrow()
    {
        using var metrics = new DlqMetrics();

        metrics.RecordDlqMessage("orders", "connect-sink", 1024);
        metrics.RecordRoutingFailure("orders", "connect-sink", "TimeoutException");
        metrics.RecordRoutingLatency("orders", 15.5);
    }

    [Fact]
    public void DlqMetrics_Dispose_DoesNotThrow()
    {
        var metrics = new DlqMetrics();
        metrics.Dispose();
        // Double dispose should be safe
        metrics.Dispose();
    }

    #endregion

    #region Helpers

    private static DlqRecord CreateTestDlqRecord(string topic = "test-topic") => new()
    {
        OriginalTopic = topic,
        OriginalPartition = 0,
        OriginalOffset = 0,
        OriginalKey = [1, 2, 3],
        OriginalValue = [4, 5, 6],
        ExceptionType = "InvalidOperationException",
        ExceptionMessage = "Test error",
        SourceName = "test-source",
        SourceType = "consumer",
        AttemptCount = 3,
        FailedAt = DateTimeOffset.UtcNow
    };

    private sealed class FakeDlqProducer : IDlqProducer
    {
        public List<(string Topic, byte[]? Key, byte[] Value)> ProducedMessages { get; } = [];
        public List<(string Topic, int Partitions)> EnsureTopicCalls { get; } = [];

        public Task ProduceAsync(string topic, byte[]? key, byte[] value, CancellationToken cancellationToken = default)
        {
            ProducedMessages.Add((topic, key, value));
            return Task.CompletedTask;
        }

        public Task EnsureTopicExistsAsync(string topic, int partitionCount, Dictionary<string, string>? config = null,
            CancellationToken cancellationToken = default)
        {
            EnsureTopicCalls.Add((topic, partitionCount));
            return Task.CompletedTask;
        }
    }

    #endregion
}

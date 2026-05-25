using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Dlq;
using Kuestenlogik.Surgewave.Client.Serialization;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for SurgewaveProducerOptions, SurgewaveConsumerOptions, DeliveryReport, ConsumeResult,
/// ProduceResult, TopicPartitionOffset, ConsumerDlqConfig, and related model types.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ProducerConsumerConfigTests
{
    #region SurgewaveProducerOptions Tests

    [Fact]
    public void SurgewaveProducerOptions_DefaultValues()
    {
        var options = new SurgewaveProducerOptions<string, string>();

        Assert.Null(options.BootstrapServers);
        Assert.Null(options.ClientId);
        Assert.Equal(100, options.BatchSize);
        Assert.Equal(5, options.LingerMs);
        Assert.Equal(30000, options.RequestTimeoutMs);
        Assert.NotNull(options.KeySerializer);
        Assert.NotNull(options.ValueSerializer);
        Assert.Null(options.AsyncKeySerializer);
        Assert.Null(options.AsyncValueSerializer);
        Assert.Null(options.DeliveryHandler);
    }

    [Fact]
    public void SurgewaveProducerOptions_Validate_MissingBootstrapServers_Throws()
    {
        var options = new SurgewaveProducerOptions<string, string>();
        Assert.Throws<InvalidConfigurationException>(() => options.Validate());
    }

    [Fact]
    public void SurgewaveProducerOptions_Validate_InvalidBatchSize_Throws()
    {
        var options = new SurgewaveProducerOptions<string, string>
        {
            BootstrapServers = "localhost:9092",
            BatchSize = 0
        };
        Assert.Throws<InvalidConfigurationException>(() => options.Validate());
    }

    [Fact]
    public void SurgewaveProducerOptions_Validate_NegativeLingerMs_Throws()
    {
        var options = new SurgewaveProducerOptions<string, string>
        {
            BootstrapServers = "localhost:9092",
            LingerMs = -1
        };
        Assert.Throws<InvalidConfigurationException>(() => options.Validate());
    }

    [Fact]
    public void SurgewaveProducerOptions_Validate_InvalidTimeoutMs_Throws()
    {
        var options = new SurgewaveProducerOptions<string, string>
        {
            BootstrapServers = "localhost:9092",
            RequestTimeoutMs = 0
        };
        Assert.Throws<InvalidConfigurationException>(() => options.Validate());
    }

    [Fact]
    public void SurgewaveProducerOptions_Validate_ValidConfig_DoesNotThrow()
    {
        var options = new SurgewaveProducerOptions<string, string>
        {
            BootstrapServers = "localhost:9092",
            ClientId = "my-producer",
            BatchSize = 50,
            LingerMs = 10,
            RequestTimeoutMs = 5000
        };
        options.Validate(); // Should not throw
    }

    [Fact]
    public void SurgewaveProducerOptions_DefaultSerializer_String()
    {
        var options = new SurgewaveProducerOptions<string, string>();
        Assert.NotNull(options.KeySerializer);
        Assert.NotNull(options.ValueSerializer);
    }

    [Fact]
    public void SurgewaveProducerOptions_DefaultSerializer_Int32()
    {
        var options = new SurgewaveProducerOptions<int, int>();
        Assert.NotNull(options.KeySerializer);
        Assert.NotNull(options.ValueSerializer);
    }

    [Fact]
    public void SurgewaveProducerOptions_DefaultSerializer_Int64()
    {
        var options = new SurgewaveProducerOptions<long, long>();
        Assert.NotNull(options.KeySerializer);
        Assert.NotNull(options.ValueSerializer);
    }

    [Fact]
    public void SurgewaveProducerOptions_DefaultSerializer_Guid()
    {
        var options = new SurgewaveProducerOptions<Guid, Guid>();
        Assert.NotNull(options.KeySerializer);
        Assert.NotNull(options.ValueSerializer);
    }

    [Fact]
    public void SurgewaveProducerOptions_DefaultSerializer_ByteArray()
    {
        var options = new SurgewaveProducerOptions<byte[], byte[]>();
        Assert.NotNull(options.KeySerializer);
        Assert.NotNull(options.ValueSerializer);
    }

    [Fact]
    public void SurgewaveProducerOptions_DefaultSerializer_Null()
    {
        var options = new SurgewaveProducerOptions<Null, string>();
        Assert.NotNull(options.KeySerializer);
    }

    [Fact]
    public void SurgewaveProducerOptions_DefaultSerializer_ComplexType_UsesJson()
    {
        // Complex types should default to JSON serializer
        var options = new SurgewaveProducerOptions<string, TestPayload>();
        Assert.NotNull(options.ValueSerializer);

        var payload = new TestPayload { Name = "test", Value = 42 };
        var bytes = options.ValueSerializer.Serialize(payload, "topic");
        Assert.NotNull(bytes);

        // Verify it's valid JSON
        var json = System.Text.Encoding.UTF8.GetString(bytes);
        Assert.Contains("Name", json);
    }

    #endregion

    #region SurgewaveConsumerOptions Tests

    [Fact]
    public void SurgewaveConsumerOptions_DefaultValues()
    {
        var options = new SurgewaveConsumerOptions<string, string>();

        Assert.Null(options.BootstrapServers);
        Assert.Null(options.GroupId);
        Assert.Null(options.ClientId);
        Assert.Equal(AutoOffsetReset.Latest, options.AutoOffsetReset);
        Assert.True(options.EnableAutoCommit);
        Assert.Equal(5000, options.AutoCommitIntervalMs);
        Assert.Equal(300000, options.MaxPollIntervalMs);
        Assert.Equal(30000, options.SessionTimeoutMs);
        Assert.Equal(3000, options.HeartbeatIntervalMs);
        Assert.Equal(1024 * 1024, options.FetchMaxBytes);
        Assert.True(options.EnableAutoReconnect);
        Assert.Equal(10, options.MaxReconnectAttempts);
        Assert.Equal(100, options.ReconnectBackoffMs);
        Assert.Equal(10000, options.ReconnectBackoffMaxMs);
        Assert.Equal(IsolationLevel.ReadUncommitted, options.IsolationLevel);
        Assert.Equal(-1, options.TopicDiscoveryTimeoutMs);
        Assert.Equal(1000, options.TopicDiscoveryRetryIntervalMs);
    }

    [Fact]
    public void SurgewaveConsumerOptions_Validate_MissingBootstrapServers_Throws()
    {
        var options = new SurgewaveConsumerOptions<string, string>();
        Assert.Throws<InvalidConfigurationException>(() => options.Validate());
    }

    [Fact]
    public void SurgewaveConsumerOptions_Validate_ValidConfig_DoesNotThrow()
    {
        var options = new SurgewaveConsumerOptions<string, string>
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group"
        };
        options.Validate(); // Should not throw
    }

    [Fact]
    public void SurgewaveConsumerOptions_Validate_HeartbeatExceedsSession_Throws()
    {
        var options = new SurgewaveConsumerOptions<string, string>
        {
            BootstrapServers = "localhost:9092",
            SessionTimeoutMs = 5000,
            HeartbeatIntervalMs = 5000 // Must be less than session timeout
        };
        Assert.Throws<InvalidConfigurationException>(() => options.Validate());
    }

    [Fact]
    public void SurgewaveConsumerOptions_Validate_HeartbeatGreaterThanSession_Throws()
    {
        var options = new SurgewaveConsumerOptions<string, string>
        {
            BootstrapServers = "localhost:9092",
            SessionTimeoutMs = 5000,
            HeartbeatIntervalMs = 6000
        };
        Assert.Throws<InvalidConfigurationException>(() => options.Validate());
    }

    [Fact]
    public void SurgewaveConsumerOptions_DefaultDeserializer_String()
    {
        var options = new SurgewaveConsumerOptions<string, string>();
        Assert.NotNull(options.KeyDeserializer);
        Assert.NotNull(options.ValueDeserializer);
    }

    [Fact]
    public void SurgewaveConsumerOptions_DefaultDeserializer_ByteArray()
    {
        var options = new SurgewaveConsumerOptions<byte[], byte[]>();
        Assert.NotNull(options.KeyDeserializer);
        Assert.NotNull(options.ValueDeserializer);
    }

    [Fact]
    public void SurgewaveConsumerOptions_DefaultDeserializer_Int32()
    {
        var options = new SurgewaveConsumerOptions<int, int>();
        Assert.NotNull(options.KeyDeserializer);
    }

    [Fact]
    public void SurgewaveConsumerOptions_DefaultDeserializer_Int64()
    {
        var options = new SurgewaveConsumerOptions<long, long>();
        Assert.NotNull(options.KeyDeserializer);
    }

    [Fact]
    public void SurgewaveConsumerOptions_DefaultDeserializer_Guid()
    {
        var options = new SurgewaveConsumerOptions<Guid, Guid>();
        Assert.NotNull(options.KeyDeserializer);
    }

    [Fact]
    public void SurgewaveConsumerOptions_Validate_InvalidClientId_Throws()
    {
        var options = new SurgewaveConsumerOptions<string, string>
        {
            BootstrapServers = "localhost:9092",
            ClientId = new string('x', 256)
        };
        Assert.Throws<InvalidConfigurationException>(() => options.Validate());
    }

    #endregion

    #region DeliveryReport Tests

    [Fact]
    public void DeliveryReport_Success_CreatedCorrectly()
    {
        var produceResult = new ProduceResult
        {
            Topic = "events",
            Partition = 2,
            Offset = 100,
            Timestamp = DateTimeOffset.UtcNow
        };

        var report = DeliveryReport<string, string>.Success(produceResult, "key1", "value1");

        Assert.Equal(DeliveryStatus.Success, report.Status);
        Assert.Equal("events", report.Topic);
        Assert.Equal(2, report.Partition);
        Assert.Equal(100, report.Offset);
        Assert.Equal("key1", report.Key);
        Assert.Equal("value1", report.Value);
        Assert.Null(report.Error);
    }

    [Fact]
    public void DeliveryReport_Failed_CreatedCorrectly()
    {
        var error = new InvalidOperationException("Test error");
        var report = DeliveryReport<string, string>.Failed("events", "key1", "value1", error);

        Assert.Equal(DeliveryStatus.Error, report.Status);
        Assert.Equal("events", report.Topic);
        Assert.Equal(-1, report.Partition);
        Assert.Equal(-1, report.Offset);
        Assert.Equal("key1", report.Key);
        Assert.Equal("value1", report.Value);
        Assert.Same(error, report.Error);
    }

    [Fact]
    public void DeliveryReport_Success_WithHeaders()
    {
        var headers = new Dictionary<string, byte[]> { ["trace-id"] = [1, 2, 3] };
        var produceResult = new ProduceResult
        {
            Topic = "t",
            Partition = 0,
            Offset = 0,
        };

        var report = DeliveryReport<string, string>.Success(produceResult, "k", "v", headers);
        Assert.NotNull(report.Headers);
        Assert.Equal(new byte[] { 1, 2, 3 }, report.Headers["trace-id"]);
    }

    #endregion

    #region ConsumeResult Tests

    [Fact]
    public void ConsumeResult_Properties()
    {
        var now = DateTimeOffset.UtcNow;
        var headers = new Dictionary<string, byte[]> { ["h1"] = [42] };

        var result = new ConsumeResult<string, string>
        {
            Topic = "my-topic",
            Partition = 3,
            Offset = 999,
            Timestamp = now,
            Key = "key",
            Value = "value",
            Headers = headers
        };

        Assert.Equal("my-topic", result.Topic);
        Assert.Equal(3, result.Partition);
        Assert.Equal(999, result.Offset);
        Assert.Equal(now, result.Timestamp);
        Assert.Equal("key", result.Key);
        Assert.Equal("value", result.Value);
        Assert.NotNull(result.Headers);
        Assert.Equal(new byte[] { 42 }, result.Headers["h1"]);
    }

    [Fact]
    public void ConsumeResult_NullKey()
    {
        var result = new ConsumeResult<string, string>
        {
            Topic = "t",
            Partition = 0,
            Offset = 0,
            Value = "v"
        };

        Assert.Null(result.Key);
    }

    #endregion

    #region ProduceResult Tests

    [Fact]
    public void ProduceResult_Properties()
    {
        var now = DateTimeOffset.UtcNow;
        var result = new ProduceResult
        {
            Topic = "orders",
            Partition = 5,
            Offset = 12345,
            Timestamp = now
        };

        Assert.Equal("orders", result.Topic);
        Assert.Equal(5, result.Partition);
        Assert.Equal(12345, result.Offset);
        Assert.Equal(now, result.Timestamp);
    }

    [Fact]
    public void ProduceResult_RecordEquality()
    {
        var now = DateTimeOffset.UtcNow;
        var r1 = new ProduceResult { Topic = "t", Partition = 0, Offset = 1, Timestamp = now };
        var r2 = new ProduceResult { Topic = "t", Partition = 0, Offset = 1, Timestamp = now };

        Assert.Equal(r1, r2);
    }

    #endregion

    #region TopicPartitionOffset Tests

    [Fact]
    public void TopicPartitionOffset_Constructor()
    {
        var tpo = new TopicPartitionOffset("orders", 3, 42);

        Assert.Equal("orders", tpo.Topic);
        Assert.Equal(3, tpo.Partition);
        Assert.Equal(42, tpo.Offset);
    }

    [Fact]
    public void TopicPartitionOffset_Equality()
    {
        var tpo1 = new TopicPartitionOffset("topic", 1, 100);
        var tpo2 = new TopicPartitionOffset("topic", 1, 100);

        Assert.Equal(tpo1, tpo2);
    }

    [Fact]
    public void TopicPartitionOffset_Inequality_DifferentTopic()
    {
        var tpo1 = new TopicPartitionOffset("topic-a", 1, 100);
        var tpo2 = new TopicPartitionOffset("topic-b", 1, 100);

        Assert.NotEqual(tpo1, tpo2);
    }

    [Fact]
    public void TopicPartitionOffset_Inequality_DifferentPartition()
    {
        var tpo1 = new TopicPartitionOffset("topic", 0, 100);
        var tpo2 = new TopicPartitionOffset("topic", 1, 100);

        Assert.NotEqual(tpo1, tpo2);
    }

    #endregion

    #region ConsumerDlqConfig Tests

    [Fact]
    public void ConsumerDlqConfig_DefaultValues()
    {
        var config = new ConsumerDlqConfig();

        Assert.False(config.EnableDlq);
        Assert.Equal(3, config.MaxRetries);
        Assert.Equal(1000, config.RetryBackoffMs);
        Assert.Equal(".DLQ", config.TopicSuffix);
        Assert.True(config.IncludeStackTrace);
    }

    [Fact]
    public void ConsumerDlqConfig_GetDlqTopicName()
    {
        var config = new ConsumerDlqConfig();
        Assert.Equal("orders.DLQ", config.GetDlqTopicName("orders"));
    }

    [Fact]
    public void ConsumerDlqConfig_GetDlqTopicName_CustomSuffix()
    {
        var config = new ConsumerDlqConfig { TopicSuffix = "-dead-letter" };
        Assert.Equal("events-dead-letter", config.GetDlqTopicName("events"));
    }

    #endregion

    #region InvalidConfigurationException Tests

    [Fact]
    public void InvalidConfigurationException_DefaultConstructor()
    {
        var ex = new InvalidConfigurationException();
        Assert.Equal("Configuration", ex.PropertyName);
        Assert.Contains("Configuration", ex.Message);
    }

    [Fact]
    public void InvalidConfigurationException_PropertyName()
    {
        var ex = new InvalidConfigurationException("BootstrapServers");
        Assert.Equal("BootstrapServers", ex.PropertyName);
        Assert.Contains("BootstrapServers", ex.Message);
        Assert.Contains("required", ex.Message);
    }

    [Fact]
    public void InvalidConfigurationException_WithInvalidValue()
    {
        var ex = new InvalidConfigurationException("BatchSize", 0);
        Assert.Equal("BatchSize", ex.PropertyName);
        Assert.Equal(0, ex.InvalidValue);
    }

    [Fact]
    public void InvalidConfigurationException_WithReason()
    {
        var ex = new InvalidConfigurationException("BatchSize", -1, "must be positive");
        Assert.Equal("BatchSize", ex.PropertyName);
        Assert.Equal(-1, ex.InvalidValue);
        Assert.Equal("must be positive", ex.Reason);
        Assert.Contains("must be positive", ex.Message);
    }

    [Fact]
    public void InvalidConfigurationException_WithInnerException()
    {
        var inner = new FormatException("Bad format");
        var ex = new InvalidConfigurationException("parse error", inner);
        Assert.Same(inner, ex.InnerException);
        Assert.Equal("Unknown", ex.PropertyName);
    }

    [Fact]
    public void InvalidConfigurationException_IsSurgewaveClientException()
    {
        var ex = new InvalidConfigurationException("Test");
        Assert.IsAssignableFrom<SurgewaveClientException>(ex);
    }

    [Fact]
    public void InvalidConfigurationException_ImplementsIRecoverable()
    {
        var ex = new InvalidConfigurationException("BootstrapServers");
        Assert.NotNull(ex.RecoverySuggestion);
        Assert.Contains("BootstrapServers", ex.RecoverySuggestion);
    }

    #endregion

    #region SerializationException Tests

    [Fact]
    public void SerializationException_DefaultConstructor()
    {
        var ex = new SerializationException();
        Assert.NotNull(ex);
    }

    [Fact]
    public void SerializationException_WithMessage()
    {
        var ex = new SerializationException("Serialization failed");
        Assert.Equal("Serialization failed", ex.Message);
    }

    [Fact]
    public void SerializationException_WithDirection()
    {
        var ex = new SerializationException(SerializationDirection.Serialize, typeof(string), "my-topic");
        Assert.Equal(SerializationDirection.Serialize, ex.Direction);
        Assert.Equal(typeof(string), ex.TargetType);
        Assert.Equal("my-topic", ex.Topic);
        Assert.Contains("serialize", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("String", ex.Message);
        Assert.Contains("my-topic", ex.Message);
    }

    [Fact]
    public void SerializationException_Deserialize_Direction()
    {
        var ex = new SerializationException(SerializationDirection.Deserialize, typeof(int), "events");
        Assert.Equal(SerializationDirection.Deserialize, ex.Direction);
        Assert.Contains("deserialize", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Int32", ex.Message);
    }

    [Fact]
    public void SerializationException_IsSurgewaveClientException()
    {
        var ex = new SerializationException("test");
        Assert.IsAssignableFrom<SurgewaveClientException>(ex);
    }

    [Fact]
    public void SerializationException_RecoverySuggestion()
    {
        var ex = new SerializationException(SerializationDirection.Serialize, typeof(string), "topic");
        Assert.NotNull(ex.RecoverySuggestion);
        Assert.Contains("serializable", ex.RecoverySuggestion, StringComparison.OrdinalIgnoreCase);
    }

    #endregion

    #region Test Types

    private sealed class TestPayload
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    #endregion
}

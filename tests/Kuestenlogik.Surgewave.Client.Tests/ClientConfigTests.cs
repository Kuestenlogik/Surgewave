using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Producer;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for client configuration records.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ClientConfigTests
{
    #region ConsumerConfig Tests

    [Fact]
    public void ConsumerConfig_RequiredBootstrapServers_SetCorrectly()
    {
        // Act
        var config = new ConsumerConfig { BootstrapServers = "localhost:9092" };

        // Assert
        Assert.Equal("localhost:9092", config.BootstrapServers);
    }

    [Fact]
    public void ConsumerConfig_DefaultValues()
    {
        // Act
        var config = new ConsumerConfig { BootstrapServers = "localhost:9092" };

        // Assert
        Assert.Null(config.GroupId);
        Assert.Null(config.ClientId);
        Assert.True(config.EnableAutoCommit);
        Assert.Equal(5000, config.AutoCommitIntervalMs);
        Assert.Equal(1, config.FetchMinBytes);
        Assert.Equal(500, config.FetchMaxWaitMs);
        Assert.Equal(500, config.MaxPollRecords);
        Assert.Equal("latest", config.AutoOffsetReset);
    }

    [Fact]
    public void ConsumerConfig_CustomValues()
    {
        // Act
        var config = new ConsumerConfig
        {
            BootstrapServers = "broker1:9092,broker2:9092",
            GroupId = "my-consumer-group",
            ClientId = "my-consumer",
            EnableAutoCommit = false,
            AutoCommitIntervalMs = 10000,
            FetchMinBytes = 1024,
            FetchMaxWaitMs = 1000,
            MaxPollRecords = 100,
            AutoOffsetReset = "earliest"
        };

        // Assert
        Assert.Equal("broker1:9092,broker2:9092", config.BootstrapServers);
        Assert.Equal("my-consumer-group", config.GroupId);
        Assert.Equal("my-consumer", config.ClientId);
        Assert.False(config.EnableAutoCommit);
        Assert.Equal(10000, config.AutoCommitIntervalMs);
        Assert.Equal(1024, config.FetchMinBytes);
        Assert.Equal(1000, config.FetchMaxWaitMs);
        Assert.Equal(100, config.MaxPollRecords);
        Assert.Equal("earliest", config.AutoOffsetReset);
    }

    [Fact]
    public void ConsumerConfig_WithExpression_CreatesModifiedCopy()
    {
        // Arrange
        var original = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "group-1"
        };

        // Act
        var modified = original with { GroupId = "group-2" };

        // Assert
        Assert.Equal("group-1", original.GroupId);
        Assert.Equal("group-2", modified.GroupId);
        Assert.Equal("localhost:9092", modified.BootstrapServers);
    }

    [Fact]
    public void ConsumerConfig_Equality_SameValues_AreEqual()
    {
        // Arrange
        var config1 = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "group"
        };
        var config2 = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "group"
        };

        // Assert
        Assert.Equal(config1, config2);
    }

    [Fact]
    public void ConsumerConfig_Equality_DifferentValues_NotEqual()
    {
        // Arrange
        var config1 = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "group-1"
        };
        var config2 = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "group-2"
        };

        // Assert
        Assert.NotEqual(config1, config2);
    }

    #endregion

    #region ProducerConfig Tests

    [Fact]
    public void ProducerConfig_RequiredBootstrapServers_SetCorrectly()
    {
        // Act
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        // Assert
        Assert.Equal("localhost:9092", config.BootstrapServers);
    }

    [Fact]
    public void ProducerConfig_DefaultValues()
    {
        // Act
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        // Assert
        Assert.Null(config.ClientId);
        Assert.Equal(30000, config.RequestTimeoutMs);
        Assert.Equal(1, config.RequiredAcks);
        Assert.Equal(16384, config.BatchSize);
        Assert.Equal(0, config.LingerMs);
        Assert.Equal(5, config.MaxInFlightRequests);
    }

    [Fact]
    public void ProducerConfig_CustomValues()
    {
        // Act
        var config = new ProducerConfig
        {
            BootstrapServers = "broker1:9092,broker2:9092",
            ClientId = "my-producer",
            RequestTimeoutMs = 60000,
            RequiredAcks = -1,  // all replicas
            BatchSize = 32768,
            LingerMs = 5,
            MaxInFlightRequests = 1
        };

        // Assert
        Assert.Equal("broker1:9092,broker2:9092", config.BootstrapServers);
        Assert.Equal("my-producer", config.ClientId);
        Assert.Equal(60000, config.RequestTimeoutMs);
        Assert.Equal(-1, config.RequiredAcks);
        Assert.Equal(32768, config.BatchSize);
        Assert.Equal(5, config.LingerMs);
        Assert.Equal(1, config.MaxInFlightRequests);
    }

    [Fact]
    public void ProducerConfig_RequiredAcks_ZeroForFireAndForget()
    {
        // Act
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            RequiredAcks = 0  // fire and forget
        };

        // Assert
        Assert.Equal(0, config.RequiredAcks);
    }

    [Fact]
    public void ProducerConfig_RequiredAcks_NegativeOneForAll()
    {
        // Act
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            RequiredAcks = -1  // all in-sync replicas
        };

        // Assert
        Assert.Equal(-1, config.RequiredAcks);
    }

    [Fact]
    public void ProducerConfig_WithExpression_CreatesModifiedCopy()
    {
        // Arrange
        var original = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            BatchSize = 16384
        };

        // Act
        var modified = original with { BatchSize = 32768 };

        // Assert
        Assert.Equal(16384, original.BatchSize);
        Assert.Equal(32768, modified.BatchSize);
    }

    #endregion

    #region ConsumerRecord Tests

    [Fact]
    public void ConsumerRecord_RequiredProperties_SetCorrectly()
    {
        // Arrange
        var value = new byte[] { 1, 2, 3 };

        // Act
        var record = new ConsumerRecord
        {
            Topic = "my-topic",
            Partition = 2,
            Offset = 100,
            Timestamp = 1234567890,
            Value = value
        };

        // Assert
        Assert.Equal("my-topic", record.Topic);
        Assert.Equal(2, record.Partition);
        Assert.Equal(100, record.Offset);
        Assert.Equal(1234567890, record.Timestamp);
        Assert.Equal(value, record.Value);
    }

    [Fact]
    public void ConsumerRecord_OptionalKeyAndHeaders_Null()
    {
        // Act
        var record = new ConsumerRecord
        {
            Topic = "topic",
            Partition = 0,
            Offset = 0,
            Timestamp = 0,
            Value = [1]
        };

        // Assert
        Assert.Null(record.Key);
        Assert.Null(record.Headers);
    }

    [Fact]
    public void ConsumerRecord_WithKeyAndHeaders()
    {
        // Arrange
        var key = new byte[] { 1, 2 };
        var headers = new Dictionary<string, byte[]>
        {
            ["header1"] = [1, 2, 3],
            ["header2"] = [4, 5]
        };

        // Act
        var record = new ConsumerRecord
        {
            Topic = "topic",
            Partition = 0,
            Offset = 0,
            Timestamp = 0,
            Key = key,
            Value = [1],
            Headers = headers
        };

        // Assert
        Assert.Equal(key, record.Key);
        Assert.Equal(2, record.Headers!.Count);
        Assert.Equal(new byte[] { 1, 2, 3 }, record.Headers["header1"]);
    }

    #endregion

    #region ProducerRecord Tests

    [Fact]
    public void ProducerRecord_RequiredProperties_SetCorrectly()
    {
        // Arrange
        var value = new byte[] { 1, 2, 3 };

        // Act
        var record = new ProducerRecord
        {
            Topic = "my-topic",
            Value = value
        };

        // Assert
        Assert.Equal("my-topic", record.Topic);
        Assert.Equal(value, record.Value);
    }

    [Fact]
    public void ProducerRecord_OptionalProperties_Null()
    {
        // Act
        var record = new ProducerRecord
        {
            Topic = "topic",
            Value = [1]
        };

        // Assert
        Assert.Null(record.Partition);
        Assert.Null(record.Key);
        Assert.Null(record.Headers);
        Assert.Null(record.Timestamp);
    }

    [Fact]
    public void ProducerRecord_WithAllProperties()
    {
        // Arrange
        var key = new byte[] { 1 };
        var value = new byte[] { 2 };
        var headers = new Dictionary<string, byte[]> { ["key"] = [3] };

        // Act
        var record = new ProducerRecord
        {
            Topic = "topic",
            Partition = 5,
            Key = key,
            Value = value,
            Headers = headers,
            Timestamp = 9999
        };

        // Assert
        Assert.Equal("topic", record.Topic);
        Assert.Equal(5, record.Partition);
        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
        Assert.Equal(headers, record.Headers);
        Assert.Equal(9999, record.Timestamp);
    }

    #endregion

    #region RecordMetadata Tests

    [Fact]
    public void RecordMetadata_Constructor_SetsAllProperties()
    {
        // Act
        var metadata = new RecordMetadata("my-topic", 3, 42, 1234567890);

        // Assert
        Assert.Equal("my-topic", metadata.Topic);
        Assert.Equal(3, metadata.Partition);
        Assert.Equal(42, metadata.Offset);
        Assert.Equal(1234567890, metadata.Timestamp);
    }

    [Fact]
    public void RecordMetadata_Equality_SameValues_AreEqual()
    {
        // Arrange
        var meta1 = new RecordMetadata("topic", 1, 100, 12345);
        var meta2 = new RecordMetadata("topic", 1, 100, 12345);

        // Assert
        Assert.Equal(meta1, meta2);
    }

    [Fact]
    public void RecordMetadata_Equality_DifferentOffset_NotEqual()
    {
        // Arrange
        var meta1 = new RecordMetadata("topic", 1, 100, 12345);
        var meta2 = new RecordMetadata("topic", 1, 200, 12345);

        // Assert
        Assert.NotEqual(meta1, meta2);
    }

    [Fact]
    public void RecordMetadata_Deconstruction()
    {
        // Arrange
        var metadata = new RecordMetadata("events", 2, 500, 9999);

        // Act
        var (topic, partition, offset, timestamp) = metadata;

        // Assert
        Assert.Equal("events", topic);
        Assert.Equal(2, partition);
        Assert.Equal(500, offset);
        Assert.Equal(9999, timestamp);
    }

    #endregion
}

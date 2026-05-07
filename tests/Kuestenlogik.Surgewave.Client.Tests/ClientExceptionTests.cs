using System.Net.Sockets;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for Surgewave client exceptions.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ClientExceptionTests
{
    #region SurgewaveClientException Tests

    [Fact]
    public void SurgewaveClientException_DefaultConstructor()
    {
        // Act
        var ex = new SurgewaveClientException();

        // Assert
        Assert.NotNull(ex);
        Assert.Null(ex.InnerException);
    }

    [Fact]
    public void SurgewaveClientException_WithMessage()
    {
        // Act
        var ex = new SurgewaveClientException("Test message");

        // Assert
        Assert.Equal("Test message", ex.Message);
    }

    [Fact]
    public void SurgewaveClientException_WithMessageAndInnerException()
    {
        // Arrange
        var inner = new InvalidOperationException("Inner");

        // Act
        var ex = new SurgewaveClientException("Outer message", inner);

        // Assert
        Assert.Equal("Outer message", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    #endregion

    #region ProduceException Tests

    [Fact]
    public void ProduceException_DefaultConstructor()
    {
        // Act
        var ex = new ProduceException();

        // Assert
        Assert.NotNull(ex);
    }

    [Fact]
    public void ProduceException_WithMessage()
    {
        // Act
        var ex = new ProduceException("Produce failed");

        // Assert
        Assert.Equal("Produce failed", ex.Message);
        Assert.Equal(ErrorCode.Unknown, ex.ErrorCode);
    }

    [Fact]
    public void ProduceException_WithMessageAndInnerException()
    {
        // Arrange
        var inner = new IOException("Network error");

        // Act
        var ex = new ProduceException("Produce failed", inner);

        // Assert
        Assert.Equal("Produce failed", ex.Message);
        Assert.Same(inner, ex.InnerException);
        Assert.Equal(ErrorCode.Unknown, ex.ErrorCode);
    }

    [Fact]
    public void ProduceException_WithErrorCode()
    {
        // Act
        var ex = new ProduceException(ErrorCode.NotLeaderForPartition);

        // Assert
        Assert.Equal(ErrorCode.NotLeaderForPartition, ex.ErrorCode);
        Assert.Contains("NotLeaderForPartition", ex.Message);
    }

    [Fact]
    public void ProduceException_WithErrorCodeAndTopic()
    {
        // Act
        var ex = new ProduceException(ErrorCode.UnknownTopicOrPartition, topic: "test-topic");

        // Assert
        Assert.Equal(ErrorCode.UnknownTopicOrPartition, ex.ErrorCode);
        Assert.Equal("test-topic", ex.Topic);
        Assert.Null(ex.Partition);
        Assert.Contains("test-topic", ex.Message);
    }

    [Fact]
    public void ProduceException_WithErrorCodeTopicAndPartition()
    {
        // Act
        var ex = new ProduceException(ErrorCode.MessageTooLarge, topic: "test-topic", partition: 3);

        // Assert
        Assert.Equal(ErrorCode.MessageTooLarge, ex.ErrorCode);
        Assert.Equal("test-topic", ex.Topic);
        Assert.Equal(3, ex.Partition);
        Assert.Contains("test-topic-3", ex.Message);
    }

    [Fact]
    public void ProduceException_WithCustomMessageAndDetails()
    {
        // Act
        var ex = new ProduceException("Custom error", ErrorCode.CorruptMessage, "my-topic", 5);

        // Assert
        Assert.Equal("Custom error", ex.Message);
        Assert.Equal(ErrorCode.CorruptMessage, ex.ErrorCode);
        Assert.Equal("my-topic", ex.Topic);
        Assert.Equal(5, ex.Partition);
    }

    #endregion

    #region ConsumeException Tests

    [Fact]
    public void ConsumeException_DefaultConstructor()
    {
        // Act
        var ex = new ConsumeException();

        // Assert
        Assert.NotNull(ex);
    }

    [Fact]
    public void ConsumeException_WithMessage()
    {
        // Act
        var ex = new ConsumeException("Consume failed");

        // Assert
        Assert.Equal("Consume failed", ex.Message);
        Assert.Equal(ErrorCode.Unknown, ex.ErrorCode);
    }

    [Fact]
    public void ConsumeException_WithMessageAndInnerException()
    {
        // Arrange
        var inner = new TimeoutException("Timed out");

        // Act
        var ex = new ConsumeException("Consume failed", inner);

        // Assert
        Assert.Equal("Consume failed", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void ConsumeException_WithErrorCode()
    {
        // Act
        var ex = new ConsumeException(ErrorCode.OffsetOutOfRange);

        // Assert
        Assert.Equal(ErrorCode.OffsetOutOfRange, ex.ErrorCode);
        Assert.Contains("OffsetOutOfRange", ex.Message);
    }

    [Fact]
    public void ConsumeException_WithErrorCodeAndTopic()
    {
        // Act
        var ex = new ConsumeException(ErrorCode.UnknownTopicOrPartition, topic: "events");

        // Assert
        Assert.Equal(ErrorCode.UnknownTopicOrPartition, ex.ErrorCode);
        Assert.Equal("events", ex.Topic);
        Assert.Contains("events", ex.Message);
    }

    [Fact]
    public void ConsumeException_WithErrorCodeTopicAndPartition()
    {
        // Act
        var ex = new ConsumeException(ErrorCode.NotLeaderForPartition, topic: "events", partition: 2);

        // Assert
        Assert.Equal(ErrorCode.NotLeaderForPartition, ex.ErrorCode);
        Assert.Equal("events", ex.Topic);
        Assert.Equal(2, ex.Partition);
        Assert.Contains("events-2", ex.Message);
    }

    [Fact]
    public void ConsumeException_WithCustomMessageAndDetails()
    {
        // Act
        var ex = new ConsumeException("Custom consume error", ErrorCode.RecordListTooLarge, "data", 0);

        // Assert
        Assert.Equal("Custom consume error", ex.Message);
        Assert.Equal(ErrorCode.RecordListTooLarge, ex.ErrorCode);
        Assert.Equal("data", ex.Topic);
        Assert.Equal(0, ex.Partition);
    }

    #endregion

    #region BrokerResponseException Tests

    [Fact]
    public void BrokerResponseException_DefaultConstructor()
    {
        // Act
        var ex = new BrokerResponseException();

        // Assert
        Assert.NotNull(ex);
    }

    [Fact]
    public void BrokerResponseException_WithMessage()
    {
        // Act
        var ex = new BrokerResponseException("Invalid response");

        // Assert
        Assert.Equal("Invalid response", ex.Message);
    }

    [Fact]
    public void BrokerResponseException_WithMessageAndInnerException()
    {
        // Arrange
        var inner = new FormatException("Bad format");

        // Act
        var ex = new BrokerResponseException("Invalid response", inner);

        // Assert
        Assert.Equal("Invalid response", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void BrokerResponseException_WithMessageAndApiName()
    {
        // Act
        var ex = new BrokerResponseException("Empty response received", "Metadata");

        // Assert
        Assert.Equal("Empty response received", ex.Message);
        Assert.Equal("Metadata", ex.ApiName);
    }

    #endregion

    #region BrokerConnectionException Tests

    [Fact]
    public void BrokerConnectionException_DefaultConstructor()
    {
        // Act
        var ex = new BrokerConnectionException();

        // Assert
        Assert.NotNull(ex);
    }

    [Fact]
    public void BrokerConnectionException_WithMessage()
    {
        // Act
        var ex = new BrokerConnectionException("Connection refused");

        // Assert
        Assert.Equal("Connection refused", ex.Message);
    }

    [Fact]
    public void BrokerConnectionException_WithMessageAndInnerException()
    {
        // Arrange
        var inner = new SocketException(10061);

        // Act
        var ex = new BrokerConnectionException("Connection refused", inner);

        // Assert
        Assert.Equal("Connection refused", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void BrokerConnectionException_WithHostAndPort()
    {
        // Act
        var ex = new BrokerConnectionException("Failed to connect", "localhost", 9092);

        // Assert
        Assert.Equal("Failed to connect", ex.Message);
        Assert.Equal("localhost", ex.Host);
        Assert.Equal(9092, ex.Port);
    }

    [Fact]
    public void BrokerConnectionException_WithHostPortAndInnerException()
    {
        // Arrange
        var inner = new TimeoutException("Connection timed out");

        // Act
        var ex = new BrokerConnectionException("Failed to connect", "broker.example.com", 19092, inner);

        // Assert
        Assert.Equal("Failed to connect", ex.Message);
        Assert.Equal("broker.example.com", ex.Host);
        Assert.Equal(19092, ex.Port);
        Assert.Same(inner, ex.InnerException);
    }

    #endregion

    #region TopicPartitionException Tests

    [Fact]
    public void TopicPartitionException_DefaultConstructor()
    {
        // Act
        var ex = new TopicPartitionException();

        // Assert
        Assert.NotNull(ex);
    }

    [Fact]
    public void TopicPartitionException_WithMessage()
    {
        // Act
        var ex = new TopicPartitionException("Topic not found");

        // Assert
        Assert.Equal("Topic not found", ex.Message);
    }

    [Fact]
    public void TopicPartitionException_WithMessageAndInnerException()
    {
        // Arrange
        var inner = new InvalidOperationException("Metadata fetch failed");

        // Act
        var ex = new TopicPartitionException("Topic not found", inner);

        // Assert
        Assert.Equal("Topic not found", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void TopicPartitionException_WithTopic()
    {
        // Act - explicitly pass null for partition to use the topic constructor
        var ex = new TopicPartitionException("my-topic", partition: null);

        // Assert
        Assert.Equal("my-topic", ex.Topic);
        Assert.Null(ex.Partition);
        Assert.Contains("my-topic", ex.Message);
        Assert.Contains("does not exist", ex.Message);
    }

    [Fact]
    public void TopicPartitionException_WithTopicAndPartition()
    {
        // Act
        var ex = new TopicPartitionException("my-topic", partition: 5);

        // Assert
        Assert.Equal("my-topic", ex.Topic);
        Assert.Equal(5, ex.Partition);
        Assert.Contains("my-topic", ex.Message);
        Assert.Contains("partition 5", ex.Message);
        Assert.Contains("does not exist", ex.Message);
    }

    #endregion

    #region Exception Hierarchy Tests

    [Fact]
    public void ProduceException_IsSurgewaveClientException()
    {
        // Act
        var ex = new ProduceException("test");

        // Assert
        Assert.IsAssignableFrom<SurgewaveClientException>(ex);
    }

    [Fact]
    public void ConsumeException_IsSurgewaveClientException()
    {
        // Act
        var ex = new ConsumeException("test");

        // Assert
        Assert.IsAssignableFrom<SurgewaveClientException>(ex);
    }

    [Fact]
    public void BrokerResponseException_IsSurgewaveClientException()
    {
        // Act
        var ex = new BrokerResponseException("test");

        // Assert
        Assert.IsAssignableFrom<SurgewaveClientException>(ex);
    }

    [Fact]
    public void BrokerConnectionException_IsSurgewaveClientException()
    {
        // Act
        var ex = new BrokerConnectionException("test");

        // Assert
        Assert.IsAssignableFrom<SurgewaveClientException>(ex);
    }

    [Fact]
    public void TopicPartitionException_IsSurgewaveClientException()
    {
        // Act
        var ex = new TopicPartitionException("test");

        // Assert
        Assert.IsAssignableFrom<SurgewaveClientException>(ex);
    }

    #endregion
}

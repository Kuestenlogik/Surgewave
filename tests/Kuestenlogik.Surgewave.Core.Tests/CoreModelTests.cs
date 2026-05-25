using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for core models: TopicPartition, Message, MessageBatch, etc.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CoreModelTests
{
    #region TopicPartition Tests

    [Fact]
    public void TopicPartition_RequiredProperties_SetCorrectly()
    {
        // Act
        var tp = new TopicPartition { Topic = "my-topic", Partition = 3 };

        // Assert
        Assert.Equal("my-topic", tp.Topic);
        Assert.Equal(3, tp.Partition);
    }

    [Fact]
    public void TopicPartition_ToString_FormatsCorrectly()
    {
        // Arrange
        var tp = new TopicPartition { Topic = "events", Partition = 5 };

        // Act
        var result = tp.ToString();

        // Assert
        Assert.Equal("events-5", result);
    }

    [Fact]
    public void TopicPartition_ToString_HandlesPartitionZero()
    {
        // Arrange
        var tp = new TopicPartition { Topic = "test", Partition = 0 };

        // Act
        var result = tp.ToString();

        // Assert
        Assert.Equal("test-0", result);
    }

    [Fact]
    public void TopicPartition_Equality_SameValues_AreEqual()
    {
        // Arrange
        var tp1 = new TopicPartition { Topic = "topic", Partition = 1 };
        var tp2 = new TopicPartition { Topic = "topic", Partition = 1 };

        // Assert
        Assert.Equal(tp1, tp2);
        Assert.True(tp1 == tp2);
    }

    [Fact]
    public void TopicPartition_Equality_DifferentTopic_NotEqual()
    {
        // Arrange
        var tp1 = new TopicPartition { Topic = "topic-a", Partition = 1 };
        var tp2 = new TopicPartition { Topic = "topic-b", Partition = 1 };

        // Assert
        Assert.NotEqual(tp1, tp2);
        Assert.True(tp1 != tp2);
    }

    [Fact]
    public void TopicPartition_Equality_DifferentPartition_NotEqual()
    {
        // Arrange
        var tp1 = new TopicPartition { Topic = "topic", Partition = 1 };
        var tp2 = new TopicPartition { Topic = "topic", Partition = 2 };

        // Assert
        Assert.NotEqual(tp1, tp2);
    }

    [Fact]
    public void TopicPartition_GetHashCode_SameValues_SameHash()
    {
        // Arrange
        var tp1 = new TopicPartition { Topic = "topic", Partition = 1 };
        var tp2 = new TopicPartition { Topic = "topic", Partition = 1 };

        // Assert
        Assert.Equal(tp1.GetHashCode(), tp2.GetHashCode());
    }

    [Fact]
    public void TopicPartition_CanBeUsedAsHashSetKey()
    {
        // Arrange
        var set = new HashSet<TopicPartition>
        {
            new() { Topic = "a", Partition = 0 },
            new() { Topic = "a", Partition = 1 },
            new() { Topic = "b", Partition = 0 }
        };

        // Act - add duplicate
        var added = set.Add(new TopicPartition { Topic = "a", Partition = 0 });

        // Assert
        Assert.False(added);
        Assert.Equal(3, set.Count);
    }

    [Fact]
    public void TopicPartition_CanBeUsedAsDictionaryKey()
    {
        // Arrange
        var dict = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition { Topic = "a", Partition = 0 }] = 100L
        };

        // Act
        var key = new TopicPartition { Topic = "a", Partition = 0 };
        var exists = dict.TryGetValue(key, out var value);

        // Assert
        Assert.True(exists);
        Assert.Equal(100L, value);
    }

    #endregion

    #region Message Tests

    [Fact]
    public void Message_RequiredProperties_SetCorrectly()
    {
        // Arrange
        var key = new byte[] { 1, 2, 3 };
        var value = new byte[] { 4, 5, 6, 7 };
        var headers = new byte[] { 8, 9 };

        // Act
        var msg = new Message
        {
            Offset = 42,
            Timestamp = 1234567890,
            Key = key,
            Value = value,
            Headers = headers
        };

        // Assert
        Assert.Equal(42, msg.Offset);
        Assert.Equal(1234567890, msg.Timestamp);
        Assert.Equal(key, msg.Key.ToArray());
        Assert.Equal(value, msg.Value.ToArray());
        Assert.Equal(headers, msg.Headers.ToArray());
    }

    [Fact]
    public void Message_TotalSize_CalculatesCorrectly()
    {
        // Arrange
        var key = new byte[] { 1, 2, 3 };       // 3 bytes
        var value = new byte[] { 4, 5, 6, 7 };  // 4 bytes
        var headers = new byte[] { 8, 9 };      // 2 bytes

        var msg = new Message
        {
            Offset = 0,
            Timestamp = 0,
            Key = key,
            Value = value,
            Headers = headers
        };

        // Expected: 8 (offset) + 8 (timestamp) + 4 (keyLen) + 3 + 4 (valueLen) + 4 + 4 (headersLen) + 2 = 37
        var expectedSize = 8 + 8 + 4 + 3 + 4 + 4 + 4 + 2;

        // Act
        var totalSize = msg.TotalSize;

        // Assert
        Assert.Equal(expectedSize, totalSize);
    }

    [Fact]
    public void Message_TotalSize_EmptyArrays_CalculatesCorrectly()
    {
        // Arrange
        var msg = new Message
        {
            Offset = 0,
            Timestamp = 0,
            Key = ReadOnlyMemory<byte>.Empty,
            Value = ReadOnlyMemory<byte>.Empty,
            Headers = ReadOnlyMemory<byte>.Empty
        };

        // Expected: 8 + 8 + 4 + 0 + 4 + 0 + 4 + 0 = 28
        var expectedSize = 8 + 8 + 4 + 0 + 4 + 0 + 4 + 0;

        // Act
        var totalSize = msg.TotalSize;

        // Assert
        Assert.Equal(expectedSize, totalSize);
    }

    [Fact]
    public void Message_TotalSize_LargePayload_CalculatesCorrectly()
    {
        // Arrange
        var largeValue = new byte[10000];
        var msg = new Message
        {
            Offset = 0,
            Timestamp = 0,
            Key = ReadOnlyMemory<byte>.Empty,
            Value = largeValue,
            Headers = ReadOnlyMemory<byte>.Empty
        };

        // Expected: 8 + 8 + 4 + 0 + 4 + 10000 + 4 + 0 = 10028
        var expectedSize = 8 + 8 + 4 + 0 + 4 + 10000 + 4 + 0;

        // Act
        var totalSize = msg.TotalSize;

        // Assert
        Assert.Equal(expectedSize, totalSize);
    }

    [Fact]
    public void Message_Equality_SameValues_AreEqual()
    {
        // Arrange
        var key = new byte[] { 1, 2 };
        var value = new byte[] { 3, 4 };
        var headers = new byte[] { 5 };

        var msg1 = new Message
        {
            Offset = 10,
            Timestamp = 100,
            Key = key,
            Value = value,
            Headers = headers
        };

        var msg2 = new Message
        {
            Offset = 10,
            Timestamp = 100,
            Key = key,
            Value = value,
            Headers = headers
        };

        // Assert
        Assert.Equal(msg1, msg2);
    }

    [Fact]
    public void Message_Equality_DifferentOffset_NotEqual()
    {
        // Arrange
        var msg1 = new Message
        {
            Offset = 10,
            Timestamp = 100,
            Key = ReadOnlyMemory<byte>.Empty,
            Value = ReadOnlyMemory<byte>.Empty,
            Headers = ReadOnlyMemory<byte>.Empty
        };

        var msg2 = new Message
        {
            Offset = 20,
            Timestamp = 100,
            Key = ReadOnlyMemory<byte>.Empty,
            Value = ReadOnlyMemory<byte>.Empty,
            Headers = ReadOnlyMemory<byte>.Empty
        };

        // Assert
        Assert.NotEqual(msg1, msg2);
    }

    #endregion
}

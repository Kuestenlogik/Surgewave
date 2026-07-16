namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins TopicPartitionOffset construction, component-based equality (the optional
/// Error is deliberately excluded), null-safe operators, and string formatting.
/// </summary>
public class TopicPartitionOffsetTests
{
    [Fact]
    public void Constructor_SetsComponents()
    {
        var tpo = new TopicPartitionOffset("orders", 3, 42);

        Assert.Equal("orders", tpo.Topic);
        Assert.Equal(new Partition(3), tpo.Partition);
        Assert.Equal(new Offset(42), tpo.Offset);
    }

    [Fact]
    public void Constructor_NullTopic_Throws()
    {
        Assert.Throws<ArgumentNullException>("topic", () => new TopicPartitionOffset(null!, 0, 0));
    }

    [Fact]
    public void Constructor_FromTopicPartition_CopiesComponents()
    {
        var tp = new TopicPartition("orders", 5);

        var tpo = new TopicPartitionOffset(tp, 99);

        Assert.Equal("orders", tpo.Topic);
        Assert.Equal(new Partition(5), tpo.Partition);
        Assert.Equal(new Offset(99), tpo.Offset);
    }

    [Fact]
    public void TopicPartition_ReturnsTopicAndPartitionComponent()
    {
        var tpo = new TopicPartitionOffset("orders", 3, 42);
        Assert.Equal(new TopicPartition("orders", 3), tpo.TopicPartition);
    }

    [Fact]
    public void Equality_SameComponents_ReturnsTrue()
    {
        var a = new TopicPartitionOffset("orders", 1, 10);
        var b = new TopicPartitionOffset("orders", 1, 10);

        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void Equality_IgnoresError()
    {
        var plain = new TopicPartitionOffset("orders", 1, 10);
        var withError = new TopicPartitionOffset("orders", 1, 10)
        {
            Error = new Error(ErrorCode.RequestTimedOut)
        };

        Assert.Equal(plain, withError);
    }

    [Theory]
    [InlineData("other", 1, 10)]
    [InlineData("orders", 2, 10)]
    [InlineData("orders", 1, 11)]
    public void Equality_AnyDifferentComponent_ReturnsFalse(string topic, int partition, long offset)
    {
        var reference = new TopicPartitionOffset("orders", 1, 10);
        var other = new TopicPartitionOffset(topic, partition, offset);

        Assert.NotEqual(reference, other);
        Assert.True(reference != other);
    }

    [Fact]
    public void EqualityOperator_HandlesNulls()
    {
        TopicPartitionOffset? left = null;
        TopicPartitionOffset? right = null;
        var instance = new TopicPartitionOffset("orders", 0, 0);

        Assert.True(left == right);
        Assert.False(left == instance);
        Assert.False(instance == left);
        Assert.True(instance != left);
    }

    [Fact]
    public void ToString_FormatsTopicPartitionAndOffset()
    {
        var tpo = new TopicPartitionOffset("orders", 2, 100);
        Assert.Equal("orders [2] @100", tpo.ToString());
    }

    [Fact]
    public void ToString_UsesSpecialOffsetNames()
    {
        var tpo = new TopicPartitionOffset("orders", 0, Offset.End);
        Assert.Equal("orders [0] @End", tpo.ToString());
    }
}

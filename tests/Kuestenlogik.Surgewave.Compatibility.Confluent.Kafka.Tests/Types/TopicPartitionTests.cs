namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

public class TopicPartitionTests
{
    [Fact]
    public void Constructor_SetsProperties()
    {
        var tp = new TopicPartition("my-topic", 3);
        Assert.Equal("my-topic", tp.Topic);
        Assert.Equal(new Partition(3), tp.Partition);
    }

    [Fact]
    public void Equality_SameValues_ReturnsTrue()
    {
        var tp1 = new TopicPartition("topic", 1);
        var tp2 = new TopicPartition("topic", 1);
        Assert.Equal(tp1, tp2);
        Assert.True(tp1 == tp2);
    }

    [Fact]
    public void Equality_DifferentTopic_ReturnsFalse()
    {
        var tp1 = new TopicPartition("topic1", 1);
        var tp2 = new TopicPartition("topic2", 1);
        Assert.NotEqual(tp1, tp2);
        Assert.True(tp1 != tp2);
    }

    [Fact]
    public void Equality_DifferentPartition_ReturnsFalse()
    {
        var tp1 = new TopicPartition("topic", 1);
        var tp2 = new TopicPartition("topic", 2);
        Assert.NotEqual(tp1, tp2);
    }

    [Fact]
    public void ToString_ReturnsFormattedString()
    {
        var tp = new TopicPartition("my-topic", 5);
        Assert.Equal("my-topic [5]", tp.ToString());  // Note: space before bracket
    }

    [Fact]
    public void GetHashCode_SameValues_SameHash()
    {
        var tp1 = new TopicPartition("topic", 1);
        var tp2 = new TopicPartition("topic", 1);
        Assert.Equal(tp1.GetHashCode(), tp2.GetHashCode());
    }
}

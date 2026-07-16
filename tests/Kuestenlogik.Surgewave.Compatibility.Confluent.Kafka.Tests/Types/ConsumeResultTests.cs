namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins ConsumeResult defaults, the composed TopicPartitionOffset, and
/// init-property round-trips including the end-of-partition marker.
/// </summary>
public class ConsumeResultTests
{
    [Fact]
    public void Defaults_AreEmptyTopicNoMessageNoEof()
    {
        var result = new ConsumeResult<string, string>();

        Assert.Equal(string.Empty, result.Topic);
        Assert.Null(result.Message);
        Assert.False(result.IsPartitionEOF);
    }

    [Fact]
    public void TopicPartitionOffset_ComposesFromComponents()
    {
        var result = new ConsumeResult<string, string>
        {
            Topic = "orders",
            Partition = 4,
            Offset = 200
        };

        Assert.Equal(new TopicPartitionOffset("orders", 4, 200), result.TopicPartitionOffset);
    }

    [Fact]
    public void InitProperties_RoundTrip()
    {
        var message = new Message<string, string> { Key = "k", Value = "v" };

        var result = new ConsumeResult<string, string>
        {
            Topic = "orders",
            Partition = 1,
            Offset = 7,
            Timestamp = new Timestamp(1000),
            Message = message,
            IsPartitionEOF = true
        };

        Assert.Same(message, result.Message);
        Assert.Equal(new Timestamp(1000), result.Timestamp);
        Assert.True(result.IsPartitionEOF);
    }
}

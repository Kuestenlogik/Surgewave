namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins DeliveryResult defaults (Status = Persisted, empty topic), the composed
/// TopicPartitionOffset, DeliveryReport substitutability, and the numeric
/// PersistenceStatus contract.
/// </summary>
public class DeliveryResultTests
{
    [Fact]
    public void Defaults_AreEmptyTopicAndPersisted()
    {
        var result = new DeliveryResult<string, string>();

        Assert.Equal(string.Empty, result.Topic);
        Assert.Equal(PersistenceStatus.Persisted, result.Status);
        Assert.Null(result.Error);
        Assert.Null(result.Message);
    }

    [Fact]
    public void TopicPartitionOffset_ComposesFromComponents()
    {
        var result = new DeliveryResult<string, string>
        {
            Topic = "orders",
            Partition = 2,
            Offset = 100
        };

        Assert.Equal(new TopicPartitionOffset("orders", 2, 100), result.TopicPartitionOffset);
    }

    [Fact]
    public void InitProperties_RoundTrip()
    {
        var message = new Message<string, string> { Key = "k", Value = "v" };
        var error = new Error(ErrorCode.RequestTimedOut);

        var result = new DeliveryResult<string, string>
        {
            Topic = "orders",
            Partition = 1,
            Offset = 7,
            Timestamp = new Timestamp(1000, TimestampType.LogAppendTime),
            Message = message,
            Error = error,
            Status = PersistenceStatus.PossiblyPersisted
        };

        Assert.Same(message, result.Message);
        Assert.Same(error, result.Error);
        Assert.Equal(new Timestamp(1000, TimestampType.LogAppendTime), result.Timestamp);
        Assert.Equal(PersistenceStatus.PossiblyPersisted, result.Status);
    }

    [Fact]
    public void DeliveryReport_IsADeliveryResult()
    {
        var report = new DeliveryReport<string, string> { Topic = "orders" };

        DeliveryResult<string, string> asResult = report;

        Assert.Same(report, asResult);
        Assert.Equal("orders", asResult.Topic);
    }

    [Theory]
    [InlineData(PersistenceStatus.NotPersisted, 0)]
    [InlineData(PersistenceStatus.PossiblyPersisted, 1)]
    [InlineData(PersistenceStatus.Persisted, 2)]
    public void PersistenceStatus_MatchesConfluentNumericValue(PersistenceStatus status, int expected)
    {
        Assert.Equal(expected, (int)status);
    }
}

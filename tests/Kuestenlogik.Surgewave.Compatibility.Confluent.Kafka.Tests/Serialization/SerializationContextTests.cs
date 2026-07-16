namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Serialization;

/// <summary>
/// Pins SerializationContext construction: component/topic assignment, the
/// never-null Headers guarantee, and the Key/Value component numeric contract.
/// </summary>
public class SerializationContextTests
{
    [Fact]
    public void Constructor_SetsComponentAndTopic()
    {
        var context = new SerializationContext(MessageComponentType.Value, "orders");

        Assert.Equal(MessageComponentType.Value, context.Component);
        Assert.Equal("orders", context.Topic);
    }

    [Fact]
    public void Constructor_NullHeaders_DefaultsToEmptyHeaders()
    {
        var context = new SerializationContext(MessageComponentType.Key, "orders");

        Assert.NotNull(context.Headers);
        Assert.Empty(context.Headers);
    }

    [Fact]
    public void Constructor_ProvidedHeaders_AreKept()
    {
        var headers = new Headers { { "trace-id", [1, 2, 3] } };

        var context = new SerializationContext(MessageComponentType.Key, "orders", headers);

        Assert.Same(headers, context.Headers);
    }

    [Theory]
    [InlineData(MessageComponentType.Key, 0)]
    [InlineData(MessageComponentType.Value, 1)]
    public void MessageComponentType_MatchesConfluentNumericValue(MessageComponentType component, int expected)
    {
        Assert.Equal(expected, (int)component);
    }
}

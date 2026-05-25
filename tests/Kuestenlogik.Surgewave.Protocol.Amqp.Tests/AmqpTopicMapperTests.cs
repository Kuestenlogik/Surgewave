using Kuestenlogik.Surgewave.Protocol.Amqp;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Amqp.Tests;

/// <summary>
/// Tests for AmqpTopicMapper — exchange/routing-key → Surgewave topic mapping.
/// </summary>
public sealed class AmqpTopicMapperTests
{
    // -------------------------------------------------------------------------
    // Direct exchange
    // -------------------------------------------------------------------------

    [Fact]
    public void DirectExchange_RoutingKey_BecomesTopic()
    {
        var topic = AmqpTopicMapper.MapToSurgewaveTopic("amq.direct", "orders", AmqpExchangeType.Direct);
        Assert.Equal("orders", topic);
    }

    [Fact]
    public void DirectExchange_EmptyRoutingKey_FallsBackToExchangeName()
    {
        var topic = AmqpTopicMapper.MapToSurgewaveTopic("my-exchange", "", AmqpExchangeType.Direct);
        Assert.Equal("my-exchange", topic);
    }

    [Fact]
    public void DirectExchange_DefaultExchange_EmptyBothUsesDefault()
    {
        var topic = AmqpTopicMapper.MapToSurgewaveTopic("", "", AmqpExchangeType.Direct);
        Assert.Equal("default", topic);
    }

    // -------------------------------------------------------------------------
    // Fanout exchange
    // -------------------------------------------------------------------------

    [Fact]
    public void FanoutExchange_ExchangeNameBecomesTopic_RoutingKeyIgnored()
    {
        var topic = AmqpTopicMapper.MapToSurgewaveTopic("logs", "ignored.key", AmqpExchangeType.Fanout);
        Assert.Equal("logs", topic);
    }

    [Fact]
    public void FanoutExchange_EmptyExchangeName_ReturnsDefault()
    {
        var topic = AmqpTopicMapper.MapToSurgewaveTopic("", "anything", AmqpExchangeType.Fanout);
        Assert.Equal("default", topic);
    }

    // -------------------------------------------------------------------------
    // Topic exchange
    // -------------------------------------------------------------------------

    [Fact]
    public void TopicExchange_RoutingKeyBecomesTopic()
    {
        var topic = AmqpTopicMapper.MapToSurgewaveTopic("amq.topic", "orders.europe.new", AmqpExchangeType.Topic);
        Assert.Equal("orders.europe.new", topic);
    }

    [Fact]
    public void TopicExchange_EmptyRoutingKey_FallsBackToExchange()
    {
        var topic = AmqpTopicMapper.MapToSurgewaveTopic("events", "", AmqpExchangeType.Topic);
        Assert.Equal("events", topic);
    }

    // -------------------------------------------------------------------------
    // Queue → consumer group
    // -------------------------------------------------------------------------

    [Fact]
    public void MapQueueToConsumerGroup_SimpleName_PassesThrough()
    {
        var group = AmqpTopicMapper.MapQueueToConsumerGroup("my-queue");
        Assert.Equal("my-queue", group);
    }

    [Fact]
    public void MapQueueToConsumerGroup_SlashInName_ReplacedWithHyphen()
    {
        var group = AmqpTopicMapper.MapQueueToConsumerGroup("tenant/orders");
        Assert.Equal("tenant-orders", group);
    }

    [Fact]
    public void MapQueueToConsumerGroup_Empty_ReturnsDefault()
    {
        var group = AmqpTopicMapper.MapQueueToConsumerGroup("");
        Assert.Equal("default", group);
    }

    // -------------------------------------------------------------------------
    // Name normalization
    // -------------------------------------------------------------------------

    [Fact]
    public void NormalizeName_SlashReplaced()
    {
        var result = AmqpTopicMapper.NormalizeName("a/b/c");
        Assert.DoesNotContain("/", result);
    }

    [Fact]
    public void NormalizeName_SpaceReplaced()
    {
        var result = AmqpTopicMapper.NormalizeName("my queue");
        Assert.DoesNotContain(" ", result);
    }

    [Fact]
    public void NormalizeName_WhitespaceOnly_ReturnsDefault()
    {
        var result = AmqpTopicMapper.NormalizeName("   ");
        Assert.Equal("default", result);
    }

    [Fact]
    public void NormalizeName_Null_ReturnsDefault()
    {
        var result = AmqpTopicMapper.NormalizeName(null!);
        Assert.Equal("default", result);
    }
}

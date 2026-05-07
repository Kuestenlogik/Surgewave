using Kuestenlogik.Surgewave.Protocol.Amqp;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Amqp.Tests;

/// <summary>
/// Tests for AmqpTopicPatternMatcher — AMQP topic-exchange routing key matching.
/// </summary>
public sealed class AmqpTopicPatternMatcherTests
{
    [Fact]
    public void HashPattern_MatchesEverything()
    {
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("#", "any.routing.key"));
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("#", "single"));
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("#", ""));
    }

    [Fact]
    public void ExactPattern_MatchesExact()
    {
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("orders.new", "orders.new"));
    }

    [Fact]
    public void ExactPattern_DoesNotMatchDifferent()
    {
        Assert.False(AmqpTopicMapper.MatchesTopicPattern("orders.new", "orders.old"));
    }

    [Fact]
    public void StarWildcard_MatchesOneWord()
    {
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("orders.*", "orders.new"));
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("orders.*", "orders.cancelled"));
    }

    [Fact]
    public void StarWildcard_DoesNotMatchMultipleWords()
    {
        Assert.False(AmqpTopicMapper.MatchesTopicPattern("orders.*", "orders.new.urgent"));
    }

    [Fact]
    public void StarWildcard_AtStart()
    {
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("*.orders", "europe.orders"));
        Assert.False(AmqpTopicMapper.MatchesTopicPattern("*.orders", "orders"));
    }

    [Fact]
    public void HashWildcard_MatchesZeroOrMoreWords()
    {
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("orders.#", "orders"));
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("orders.#", "orders.new"));
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("orders.#", "orders.new.urgent"));
    }

    [Fact]
    public void MixedWildcards_StarAndHash()
    {
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("*.orders.#", "europe.orders.new"));
        Assert.True(AmqpTopicMapper.MatchesTopicPattern("*.orders.#", "europe.orders"));
        Assert.False(AmqpTopicMapper.MatchesTopicPattern("*.orders.#", "orders.new"));
    }
}

using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt.Tests;

/// <summary>
/// Additional edge case tests for MqttTopicMatcher
/// </summary>
public sealed class MqttTopicMatcherAdditionalTests
{
    [Fact]
    public void Matches_ExactMatch_ReturnsTrue()
    {
        Assert.True(MqttTopicMatcher.Matches("a/b/c", "a/b/c"));
    }

    [Fact]
    public void Matches_ExactMismatch_ReturnsFalse()
    {
        Assert.False(MqttTopicMatcher.Matches("a/b/c", "a/b/d"));
    }

    [Fact]
    public void Matches_HashWildcard_MatchesAnySingleLevel()
    {
        Assert.True(MqttTopicMatcher.Matches("#", "any"));
    }

    [Fact]
    public void Matches_HashWildcard_MatchesAnyMultiLevel()
    {
        Assert.True(MqttTopicMatcher.Matches("#", "a/b/c/d/e"));
    }

    [Fact]
    public void Matches_HashAtEnd_MatchesMultipleRemainingLevels()
    {
        Assert.True(MqttTopicMatcher.Matches("base/#", "base/a/b/c"));
    }

    [Fact]
    public void Matches_HashAtEnd_MatchesSingleRemainingLevel()
    {
        Assert.True(MqttTopicMatcher.Matches("base/#", "base/only"));
    }

    [Fact]
    public void Matches_HashAtEnd_DoesNotMatchDifferentBase()
    {
        Assert.False(MqttTopicMatcher.Matches("base/#", "other/a/b"));
    }

    [Fact]
    public void Matches_PlusSingleLevel_MatchesAnyOneSegment()
    {
        Assert.True(MqttTopicMatcher.Matches("+", "anything"));
    }

    [Fact]
    public void Matches_PlusSingleLevel_DoesNotMatchTwoSegments()
    {
        Assert.False(MqttTopicMatcher.Matches("+", "a/b"));
    }

    [Fact]
    public void Matches_PlusInMiddle_MatchesAnySegment()
    {
        Assert.True(MqttTopicMatcher.Matches("a/+/c", "a/anything/c"));
    }

    [Fact]
    public void Matches_PlusInMiddle_DoesNotMatchExtraLevel()
    {
        Assert.False(MqttTopicMatcher.Matches("a/+/c", "a/b/extra/c"));
    }

    [Fact]
    public void Matches_MultiplePlus_MatchesCorrectly()
    {
        Assert.True(MqttTopicMatcher.Matches("+/+/+", "a/b/c"));
        Assert.False(MqttTopicMatcher.Matches("+/+/+", "a/b"));
        Assert.False(MqttTopicMatcher.Matches("+/+/+", "a/b/c/d"));
    }

    [Fact]
    public void Matches_FilterLongerThanTopic_ReturnsFalse()
    {
        Assert.False(MqttTopicMatcher.Matches("a/b/c/d", "a/b/c"));
    }

    [Fact]
    public void Matches_TopicLongerThanFilter_ReturnsFalse()
    {
        Assert.False(MqttTopicMatcher.Matches("a/b/c", "a/b/c/d"));
    }

    [Fact]
    public void Matches_CaseSensitive_DifferentCase_ReturnsFalse()
    {
        Assert.False(MqttTopicMatcher.Matches("Sensors/Temperature", "sensors/temperature"));
    }

    [Fact]
    public void Matches_CaseSensitive_SameCase_ReturnsTrue()
    {
        Assert.True(MqttTopicMatcher.Matches("Sensors/Temperature", "Sensors/Temperature"));
    }
}

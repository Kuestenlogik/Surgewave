using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt.Tests;

/// <summary>
/// Pins MQTT 3.1.1 / 5.0 topic-filter edge semantics of <see cref="MqttTopicMatcher"/>:
/// '#' also matches its parent level, '+' matches empty levels but requires the level to exist,
/// and trailing slashes create distinct empty levels.
/// </summary>
public sealed class MqttTopicMatcherSpecEdgeTests
{
    [Theory]
    // '#' matches the level it replaces AND the parent level (spec 4.7.1-1: "sport/#" matches "sport").
    [InlineData("sport/#", "sport", true)]
    [InlineData("sport/tennis/#", "sport/tennis", true)]
    [InlineData("a/b/#", "a", false)]
    [InlineData("+/#", "a", true)]
    // '+' matches an empty level but the level must exist.
    [InlineData("a/+/c", "a//c", true)]
    [InlineData("+", "", true)]
    [InlineData("sensors/+", "sensors", false)]
    [InlineData("sensors/+/+", "sensors/a", false)]
    // Empty levels are real levels: trailing slash makes the topic one level longer.
    [InlineData("a/b", "a/b/", false)]
    [InlineData("a//c", "a//c", true)]
    [InlineData("#", "", true)]
    public void Matches_SpecEdgeCases_ReturnsExpected(string filter, string topic, bool expected)
    {
        Assert.Equal(expected, MqttTopicMatcher.Matches(filter, topic));
    }

    [Fact]
    public void Matches_HashBeforeLastLevel_IsTreatedAsMatchRemainder()
    {
        // Filters are not validated: a '#' that is not the last level (invalid per spec)
        // is treated leniently as "match everything from here on".
        Assert.True(MqttTopicMatcher.Matches("a/#/c", "a/x/y"));
        Assert.False(MqttTopicMatcher.Matches("a/#/c", "b/x/y"));
    }
}

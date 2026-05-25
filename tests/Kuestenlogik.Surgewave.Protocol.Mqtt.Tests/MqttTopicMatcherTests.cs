using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt.Tests;

public sealed class MqttTopicMatcherTests
{
    [Theory]
    [InlineData("sensors/temperature", "sensors/temperature", true)]
    [InlineData("sensors/temperature", "sensors/humidity", false)]
    [InlineData("#", "any/topic/at/all", true)]
    [InlineData("sensors/#", "sensors/temperature", true)]
    [InlineData("sensors/#", "sensors/temp/room1", true)]
    [InlineData("sensors/#", "alerts/fire", false)]
    [InlineData("+/temperature", "sensors/temperature", true)]
    [InlineData("+/temperature", "devices/temperature", true)]
    [InlineData("+/temperature", "sensors/humidity", false)]
    [InlineData("sensors/+/reading", "sensors/abc/reading", true)]
    [InlineData("sensors/+/reading", "sensors/abc/def/reading", false)]
    [InlineData("+/+", "a/b", true)]
    [InlineData("+/+", "a/b/c", false)]
    public void Matches_VariousPatterns_ReturnsExpected(string filter, string topic, bool expected)
    {
        Assert.Equal(expected, MqttTopicMatcher.Matches(filter, topic));
    }
}

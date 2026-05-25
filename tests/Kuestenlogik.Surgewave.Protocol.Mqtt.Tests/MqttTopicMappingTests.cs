using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt.Tests;

/// <summary>
/// Tests for MQTT topic mapping to Surgewave topics
/// </summary>
public sealed class MqttTopicMappingTests
{
    [Theory]
    [InlineData("sensors/temperature", "mqtt.", "mqtt.sensors.temperature")]
    [InlineData("sensors/temp/room1", "mqtt.", "mqtt.sensors.temp.room1")]
    [InlineData("devices/abc/status", "iot.", "iot.devices.abc.status")]
    [InlineData("simple", "mqtt.", "mqtt.simple")]
    [InlineData("a/b/c/d/e", "", "a.b.c.d.e")]
    [InlineData("home/living-room/light", "mqtt.", "mqtt.home.living-room.light")]
    public void MapMqttToSurgewaveTopic_Standard_ReturnsExpected(
        string mqttTopic, string prefix, string expected)
    {
        var result = MqttProtocolAdapter.MapMqttToSurgewaveTopic(mqttTopic, prefix);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void MapMqttToSurgewaveTopic_EmptyPrefix_NoPrefix()
    {
        var result = MqttProtocolAdapter.MapMqttToSurgewaveTopic("a/b/c", "");
        Assert.Equal("a.b.c", result);
    }

    [Fact]
    public void MapMqttToSurgewaveTopic_SingleSegment_NoSlash()
    {
        var result = MqttProtocolAdapter.MapMqttToSurgewaveTopic("single", "ns.");
        Assert.Equal("ns.single", result);
    }

    [Fact]
    public void MapMqttToSurgewaveTopic_DeepHierarchy_AllSlashesReplaced()
    {
        var result = MqttProtocolAdapter.MapMqttToSurgewaveTopic("a/b/c/d/e/f/g", "prefix.");
        Assert.Equal("prefix.a.b.c.d.e.f.g", result);
    }

    [Fact]
    public void MapMqttToSurgewaveTopic_TopicWithDashes_DashesPreserved()
    {
        var result = MqttProtocolAdapter.MapMqttToSurgewaveTopic("sensors/living-room/temperature", "mqtt.");
        Assert.Equal("mqtt.sensors.living-room.temperature", result);
    }

    [Fact]
    public void MapMqttToSurgewaveTopic_TopicWithNumbers_NumbersPreserved()
    {
        var result = MqttProtocolAdapter.MapMqttToSurgewaveTopic("device/123/data", "mqtt.");
        Assert.Equal("mqtt.device.123.data", result);
    }

    [Fact]
    public void MapMqttToSurgewaveTopic_SpecialChars_UnderscorePreserved()
    {
        var result = MqttProtocolAdapter.MapMqttToSurgewaveTopic("some_topic/sub_topic", "");
        Assert.Equal("some_topic.sub_topic", result);
    }

    [Fact]
    public void MapMqttToSurgewaveTopic_DoesNotContainForwardSlash_After()
    {
        var result = MqttProtocolAdapter.MapMqttToSurgewaveTopic("a/b/c/d", "pfx.");
        Assert.DoesNotContain("/", result);
    }

    [Fact]
    public void MapMqttToSurgewaveTopic_PrefixAppendedAtStart()
    {
        var result = MqttProtocolAdapter.MapMqttToSurgewaveTopic("topic", "mqtt.");
        Assert.StartsWith("mqtt.", result);
    }
}

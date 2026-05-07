using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt.Tests;

public sealed class MqttProtocolAdapterTests
{
    [Theory]
    [InlineData("sensors/temperature", "mqtt.", "mqtt.sensors.temperature")]
    [InlineData("sensors/temp/room1", "mqtt.", "mqtt.sensors.temp.room1")]
    [InlineData("devices/abc/status", "iot.", "iot.devices.abc.status")]
    [InlineData("simple", "mqtt.", "mqtt.simple")]
    [InlineData("a/b/c/d/e", "", "a.b.c.d.e")]
    [InlineData("home/living-room/light", "mqtt.", "mqtt.home.living-room.light")]
    public void MapMqttToSurgewaveTopic_VariousInputs_ReturnsExpected(
        string mqttTopic, string prefix, string expected)
    {
        var result = MqttProtocolAdapter.MapMqttToSurgewaveTopic(mqttTopic, prefix);
        Assert.Equal(expected, result);
    }
}

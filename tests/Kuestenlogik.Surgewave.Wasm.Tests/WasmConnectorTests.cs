using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Wasm.Tests;

/// <summary>
/// Tests for WasmSinkConnector and WasmSourceConnector metadata and configuration.
/// </summary>
public sealed class WasmConnectorTests
{
    [Fact]
    public void WasmSinkConnector_Version_IsSet()
    {
        var connector = new WasmSinkConnector();

        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void WasmSinkConnector_TaskClass_IsWasmSinkTask()
    {
        var connector = new WasmSinkConnector();

        Assert.Equal(typeof(WasmSinkTask), connector.TaskClass);
    }

    [Fact]
    public void WasmSinkConnector_Config_HasRequiredKeys()
    {
        var connector = new WasmSinkConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == "wasm.plugin.id");
        Assert.Contains(connector.Config.Keys, k => k.Name == "wasm.plugin.path");
        Assert.Contains(connector.Config.Keys, k => k.Name == "topics");
    }

    [Fact]
    public void WasmSinkConnector_Config_ImportanceLevels()
    {
        var connector = new WasmSinkConnector();

        var pluginIdKey = connector.Config.Keys.First(k => k.Name == "wasm.plugin.id");
        var pluginPathKey = connector.Config.Keys.First(k => k.Name == "wasm.plugin.path");
        var topicsKey = connector.Config.Keys.First(k => k.Name == "topics");

        Assert.Equal(Importance.High, pluginIdKey.Importance);
        Assert.Equal(Importance.Medium, pluginPathKey.Importance);
        Assert.Equal(Importance.High, topicsKey.Importance);
    }

    [Fact]
    public void WasmSinkConnector_Metadata_IsSet()
    {
        var attr = typeof(WasmSinkConnector)
            .GetCustomAttributes(typeof(Connect.ConnectorMetadataAttribute), false)
            .Cast<Connect.ConnectorMetadataAttribute>()
            .FirstOrDefault();

        Assert.NotNull(attr);
        Assert.Equal("WASM Sink", attr.Name);
        Assert.Contains("sink", attr.Tags!);
        Assert.Equal("Memory", attr.Icon);
    }

    [Fact]
    public void WasmSinkConnector_Start_And_TaskConfigs()
    {
        var connector = new WasmSinkConnector();
        var config = new Dictionary<string, string>
        {
            ["wasm.plugin.id"] = "test-plugin",
            ["topics"] = "input-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-plugin", taskConfigs[0]["wasm.plugin.id"]);
        Assert.Equal("input-topic", taskConfigs[0]["topics"]);
    }

    [Fact]
    public void WasmSinkConnector_Stop_DoesNotThrow()
    {
        var connector = new WasmSinkConnector();
        connector.Stop();
    }

    [Fact]
    public void WasmSourceConnector_Version_IsSet()
    {
        var connector = new WasmSourceConnector();

        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void WasmSourceConnector_TaskClass_IsWasmSourceTask()
    {
        var connector = new WasmSourceConnector();

        Assert.Equal(typeof(WasmSourceTask), connector.TaskClass);
    }

    [Fact]
    public void WasmSourceConnector_Config_HasRequiredKeys()
    {
        var connector = new WasmSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == "wasm.plugin.id");
        Assert.Contains(connector.Config.Keys, k => k.Name == "wasm.plugin.path");
        Assert.Contains(connector.Config.Keys, k => k.Name == "topic");
        Assert.Contains(connector.Config.Keys, k => k.Name == "poll.interval.ms");
    }

    [Fact]
    public void WasmSourceConnector_Metadata_IsSet()
    {
        var attr = typeof(WasmSourceConnector)
            .GetCustomAttributes(typeof(Connect.ConnectorMetadataAttribute), false)
            .Cast<Connect.ConnectorMetadataAttribute>()
            .FirstOrDefault();

        Assert.NotNull(attr);
        Assert.Equal("WASM Source", attr.Name);
        Assert.Contains("source", attr.Tags!);
        Assert.Equal("Memory", attr.Icon);
    }

    [Fact]
    public void WasmSourceConnector_Start_And_TaskConfigs()
    {
        var connector = new WasmSourceConnector();
        var config = new Dictionary<string, string>
        {
            ["wasm.plugin.id"] = "sensor-source",
            ["topic"] = "sensor-data",
            ["poll.interval.ms"] = "500"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
        Assert.Equal("sensor-source", taskConfigs[0]["wasm.plugin.id"]);
        Assert.Equal("sensor-data", taskConfigs[0]["topic"]);
        Assert.Equal("500", taskConfigs[0]["poll.interval.ms"]);
    }

    [Fact]
    public void WasmSourceConnector_Stop_DoesNotThrow()
    {
        var connector = new WasmSourceConnector();
        connector.Stop();
    }
}

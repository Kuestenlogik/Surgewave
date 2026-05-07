namespace Kuestenlogik.Surgewave.Wasm.Tests;

/// <summary>
/// Tests for WasmAbi constant definitions.
/// The WasmAbi class is internal, so we verify it through reflection
/// and through types that reference its constants.
/// </summary>
public sealed class WasmAbiTests
{
    [Fact]
    public void WasmAbi_Type_Exists()
    {
        var type = typeof(WasmPluginManager).Assembly.GetType("Kuestenlogik.Surgewave.Wasm.WasmAbi");

        Assert.NotNull(type);
        Assert.True(type.IsAbstract && type.IsSealed, "WasmAbi should be a static class");
    }

    [Fact]
    public void WasmAbi_PluginExportConstants_Exist()
    {
        var type = typeof(WasmPluginManager).Assembly.GetType("Kuestenlogik.Surgewave.Wasm.WasmAbi")!;

        var pluginInit = type.GetField("PluginInit", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var pluginInfo = type.GetField("PluginInfo", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var pluginProcess = type.GetField("PluginProcess", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var pluginPoll = type.GetField("PluginPoll", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var pluginPush = type.GetField("PluginPush", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var pluginClose = type.GetField("PluginClose", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var alloc = type.GetField("Alloc", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var dealloc = type.GetField("Dealloc", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        Assert.NotNull(pluginInit);
        Assert.NotNull(pluginInfo);
        Assert.NotNull(pluginProcess);
        Assert.NotNull(pluginPoll);
        Assert.NotNull(pluginPush);
        Assert.NotNull(pluginClose);
        Assert.NotNull(alloc);
        Assert.NotNull(dealloc);

        Assert.Equal("plugin_init", pluginInit.GetValue(null));
        Assert.Equal("plugin_info", pluginInfo.GetValue(null));
        Assert.Equal("plugin_process", pluginProcess.GetValue(null));
        Assert.Equal("plugin_poll", pluginPoll.GetValue(null));
        Assert.Equal("plugin_push", pluginPush.GetValue(null));
        Assert.Equal("plugin_close", pluginClose.GetValue(null));
        Assert.Equal("alloc", alloc.GetValue(null));
        Assert.Equal("dealloc", dealloc.GetValue(null));
    }

    [Fact]
    public void WasmAbi_HostFunctionConstants_Exist()
    {
        var type = typeof(WasmPluginManager).Assembly.GetType("Kuestenlogik.Surgewave.Wasm.WasmAbi")!;

        var hostProduce = type.GetField("HostProduce", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var hostLog = type.GetField("HostLog", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var hostGetConfig = type.GetField("HostGetConfig", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var hostStateGet = type.GetField("HostStateGet", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var hostStatePut = type.GetField("HostStatePut", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        Assert.NotNull(hostProduce);
        Assert.NotNull(hostLog);
        Assert.NotNull(hostGetConfig);
        Assert.NotNull(hostStateGet);
        Assert.NotNull(hostStatePut);

        Assert.Equal("surgewave_produce", hostProduce.GetValue(null));
        Assert.Equal("surgewave_log", hostLog.GetValue(null));
        Assert.Equal("surgewave_get_config", hostGetConfig.GetValue(null));
        Assert.Equal("surgewave_state_get", hostStateGet.GetValue(null));
        Assert.Equal("surgewave_state_put", hostStatePut.GetValue(null));
    }

    [Fact]
    public void WasmAbi_AllConstants_AreStrings()
    {
        var type = typeof(WasmPluginManager).Assembly.GetType("Kuestenlogik.Surgewave.Wasm.WasmAbi")!;
        var fields = type.GetFields(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        Assert.True(fields.Length >= 13, $"Expected at least 13 constants, found {fields.Length}");

        foreach (var field in fields)
        {
            Assert.Equal(typeof(string), field.FieldType);
            var value = (string?)field.GetValue(null);
            Assert.False(string.IsNullOrWhiteSpace(value), $"Constant '{field.Name}' should not be null or whitespace");
        }
    }
}

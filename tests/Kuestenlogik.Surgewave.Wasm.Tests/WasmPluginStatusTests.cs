namespace Kuestenlogik.Surgewave.Wasm.Tests;

public sealed class WasmPluginStatusTests
{
    [Fact]
    public void Properties_AreSetCorrectly()
    {
        var loadedAt = DateTimeOffset.UtcNow;
        var status = new WasmPluginStatus(
            PluginId: "test-plugin",
            Name: "Test Plugin",
            Type: WasmPluginType.Transform,
            State: WasmPluginState.Running,
            MemoryUsageBytes: 1024 * 1024,
            MessagesProcessed: 42,
            ErrorCount: 3,
            LoadedAt: loadedAt,
            Version: "1.0.0",
            LastError: "something went wrong");

        Assert.Equal("test-plugin", status.PluginId);
        Assert.Equal("Test Plugin", status.Name);
        Assert.Equal(WasmPluginType.Transform, status.Type);
        Assert.Equal(WasmPluginState.Running, status.State);
        Assert.Equal(1024 * 1024, status.MemoryUsageBytes);
        Assert.Equal(42, status.MessagesProcessed);
        Assert.Equal(3, status.ErrorCount);
        Assert.Equal(loadedAt, status.LoadedAt);
        Assert.Equal("1.0.0", status.Version);
        Assert.Equal("something went wrong", status.LastError);
    }

    [Fact]
    public void LastError_DefaultsToNull()
    {
        var status = new WasmPluginStatus(
            PluginId: "no-error",
            Name: "Clean Plugin",
            Type: WasmPluginType.Source,
            State: WasmPluginState.Ready,
            MemoryUsageBytes: 0,
            MessagesProcessed: 0,
            ErrorCount: 0,
            LoadedAt: DateTimeOffset.UtcNow,
            Version: "1.0.0");

        Assert.Null(status.LastError);
    }

    [Fact]
    public void Record_Equality_Works()
    {
        var loadedAt = DateTimeOffset.UtcNow;
        var status1 = new WasmPluginStatus("a", "A", WasmPluginType.Sink, WasmPluginState.Ready, 0, 0, 0, loadedAt, "1.0.0");
        var status2 = new WasmPluginStatus("a", "A", WasmPluginType.Sink, WasmPluginState.Ready, 0, 0, 0, loadedAt, "1.0.0");

        Assert.Equal(status1, status2);
    }
}

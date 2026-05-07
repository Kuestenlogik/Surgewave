namespace Kuestenlogik.Surgewave.Wasm.Tests;

public sealed class WasmPluginStateTests
{
    [Fact]
    public void AllValues_Exist()
    {
        var values = Enum.GetValues<WasmPluginState>();

        Assert.Contains(WasmPluginState.Loading, values);
        Assert.Contains(WasmPluginState.Ready, values);
        Assert.Contains(WasmPluginState.Running, values);
        Assert.Contains(WasmPluginState.Failed, values);
        Assert.Contains(WasmPluginState.Stopped, values);
        Assert.Equal(5, values.Length);
    }

    [Theory]
    [InlineData(WasmPluginState.Loading, "Loading")]
    [InlineData(WasmPluginState.Ready, "Ready")]
    [InlineData(WasmPluginState.Running, "Running")]
    [InlineData(WasmPluginState.Failed, "Failed")]
    [InlineData(WasmPluginState.Stopped, "Stopped")]
    public void ToString_ReturnsExpected(WasmPluginState state, string expected)
    {
        Assert.Equal(expected, state.ToString());
    }
}

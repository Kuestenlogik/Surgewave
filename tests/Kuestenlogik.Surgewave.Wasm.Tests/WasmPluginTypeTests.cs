namespace Kuestenlogik.Surgewave.Wasm.Tests;

public sealed class WasmPluginTypeTests
{
    [Fact]
    public void AllValues_Exist()
    {
        var values = Enum.GetValues<WasmPluginType>();

        Assert.Contains(WasmPluginType.Source, values);
        Assert.Contains(WasmPluginType.Sink, values);
        Assert.Contains(WasmPluginType.Transform, values);
        Assert.Contains(WasmPluginType.Function, values);
        Assert.Equal(4, values.Length);
    }

    [Theory]
    [InlineData(WasmPluginType.Source, "Source")]
    [InlineData(WasmPluginType.Sink, "Sink")]
    [InlineData(WasmPluginType.Transform, "Transform")]
    [InlineData(WasmPluginType.Function, "Function")]
    public void ToString_ReturnsExpected(WasmPluginType type, string expected)
    {
        Assert.Equal(expected, type.ToString());
    }
}

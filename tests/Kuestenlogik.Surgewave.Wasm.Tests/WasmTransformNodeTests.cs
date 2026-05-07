namespace Kuestenlogik.Surgewave.Wasm.Tests;

public sealed class WasmTransformNodeTests
{
    [Fact]
    public void FromManifest_SetsAllProperties()
    {
        var manifest = new WasmPluginManifest
        {
            Id = "csv-transform",
            Name = "CSV Transform",
            Version = "2.0.0",
            Type = WasmPluginType.Transform,
            Description = "Transforms CSV data",
            InputTopic = "raw-csv",
            OutputTopic = "parsed-data"
        };

        var node = WasmTransformNode.FromManifest(manifest);

        Assert.Equal("csv-transform", node.PluginId);
        Assert.Equal("CSV Transform", node.Name);
        Assert.Equal("Transforms CSV data", node.Description);
        Assert.Equal(WasmPluginType.Transform, node.Type);
        Assert.Equal("raw-csv", node.InputTopic);
        Assert.Equal("parsed-data", node.OutputTopic);
    }

    [Fact]
    public void FromManifest_NullOptionalFields_AreNull()
    {
        var manifest = new WasmPluginManifest
        {
            Id = "minimal",
            Name = "Minimal",
            Version = "1.0.0",
            Type = WasmPluginType.Function
        };

        var node = WasmTransformNode.FromManifest(manifest);

        Assert.Null(node.Description);
        Assert.Null(node.InputTopic);
        Assert.Null(node.OutputTopic);
    }
}

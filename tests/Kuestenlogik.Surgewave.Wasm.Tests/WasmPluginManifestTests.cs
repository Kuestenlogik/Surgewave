using System.Text.Json;

namespace Kuestenlogik.Surgewave.Wasm.Tests;

public sealed class WasmPluginManifestTests
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    [Fact]
    public void Deserialization_FromJson_Works()
    {
        const string json = """
        {
            "id": "my-transform",
            "name": "My Transform Plugin",
            "version": "1.2.3",
            "type": "Transform",
            "description": "Transforms messages",
            "author": "Test Author",
            "inputTopic": "input-topic",
            "outputTopic": "output-topic",
            "config": {
                "key1": "value1",
                "key2": "value2"
            }
        }
        """;

        var manifest = JsonSerializer.Deserialize<WasmPluginManifest>(json, JsonOptions);

        Assert.NotNull(manifest);
        Assert.Equal("my-transform", manifest.Id);
        Assert.Equal("My Transform Plugin", manifest.Name);
        Assert.Equal("1.2.3", manifest.Version);
        Assert.Equal(WasmPluginType.Transform, manifest.Type);
        Assert.Equal("Transforms messages", manifest.Description);
        Assert.Equal("Test Author", manifest.Author);
        Assert.Equal("input-topic", manifest.InputTopic);
        Assert.Equal("output-topic", manifest.OutputTopic);
        Assert.Equal(2, manifest.Config.Count);
        Assert.Equal("value1", manifest.Config["key1"]);
        Assert.Equal("value2", manifest.Config["key2"]);
    }

    [Fact]
    public void Deserialization_MinimalManifest_Works()
    {
        const string json = """
        {
            "id": "minimal",
            "name": "Minimal Plugin",
            "version": "0.1.0",
            "type": "Source"
        }
        """;

        var manifest = JsonSerializer.Deserialize<WasmPluginManifest>(json, JsonOptions);

        Assert.NotNull(manifest);
        Assert.Equal("minimal", manifest.Id);
        Assert.Equal("Minimal Plugin", manifest.Name);
        Assert.Equal("0.1.0", manifest.Version);
        Assert.Equal(WasmPluginType.Source, manifest.Type);
        Assert.Null(manifest.Description);
        Assert.Null(manifest.Author);
        Assert.Null(manifest.InputTopic);
        Assert.Null(manifest.OutputTopic);
        Assert.Empty(manifest.Config);
    }

    [Theory]
    [InlineData("Source", WasmPluginType.Source)]
    [InlineData("Sink", WasmPluginType.Sink)]
    [InlineData("Transform", WasmPluginType.Transform)]
    [InlineData("Function", WasmPluginType.Function)]
    public void Deserialization_AllTypes_Work(string typeStr, WasmPluginType expected)
    {
        var json = $$"""
        {
            "id": "test",
            "name": "Test",
            "version": "1.0.0",
            "type": "{{typeStr}}"
        }
        """;

        var manifest = JsonSerializer.Deserialize<WasmPluginManifest>(json, JsonOptions);

        Assert.NotNull(manifest);
        Assert.Equal(expected, manifest.Type);
    }

    [Fact]
    public void Serialization_Roundtrip_Works()
    {
        var manifest = new WasmPluginManifest
        {
            Id = "roundtrip-test",
            Name = "Roundtrip",
            Version = "2.0.0",
            Type = WasmPluginType.Function,
            Description = "A roundtrip test",
            Config = new Dictionary<string, string> { ["foo"] = "bar" }
        };

        var json = JsonSerializer.Serialize(manifest, JsonOptions);
        var deserialized = JsonSerializer.Deserialize<WasmPluginManifest>(json, JsonOptions);

        Assert.NotNull(deserialized);
        Assert.Equal(manifest.Id, deserialized.Id);
        Assert.Equal(manifest.Name, deserialized.Name);
        Assert.Equal(manifest.Version, deserialized.Version);
        Assert.Equal(manifest.Type, deserialized.Type);
        Assert.Equal(manifest.Description, deserialized.Description);
        Assert.Equal("bar", deserialized.Config["foo"]);
    }
}

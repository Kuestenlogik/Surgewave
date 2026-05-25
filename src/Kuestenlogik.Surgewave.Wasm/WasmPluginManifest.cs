using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// JSON manifest that accompanies a <c>.wasm</c> file, describing the plugin's identity,
/// type, and configuration. Expected filename: <c>wasm-plugin.json</c>.
/// </summary>
public sealed class WasmPluginManifest
{
    /// <summary>Unique identifier for this plugin (e.g. <c>my-company.csv-transform</c>).</summary>
    [JsonPropertyName("id")]
    public required string Id { get; init; }

    /// <summary>Human-readable display name.</summary>
    [JsonPropertyName("name")]
    public required string Name { get; init; }

    /// <summary>Semantic version string (e.g. <c>1.2.0</c>).</summary>
    [JsonPropertyName("version")]
    public required string Version { get; init; }

    /// <summary>Plugin type — determines which ABI functions Surgewave will call.</summary>
    [JsonPropertyName("type")]
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public required WasmPluginType Type { get; init; }

    /// <summary>Optional description for the control UI and REST API.</summary>
    [JsonPropertyName("description")]
    public string? Description { get; init; }

    /// <summary>Optional author or organisation name.</summary>
    [JsonPropertyName("author")]
    public string? Author { get; init; }

    /// <summary>
    /// Input topic (for Sink / Transform / Function plugins).
    /// Ignored for Source plugins.
    /// </summary>
    [JsonPropertyName("inputTopic")]
    public string? InputTopic { get; init; }

    /// <summary>
    /// Output topic (for Source / Transform / Function plugins).
    /// Ignored for Sink plugins.
    /// </summary>
    [JsonPropertyName("outputTopic")]
    public string? OutputTopic { get; init; }

    /// <summary>
    /// Arbitrary key-value configuration passed to the WASM module
    /// via <c>surgewave_get_config</c>.
    /// </summary>
    [JsonPropertyName("config")]
    public Dictionary<string, string> Config { get; init; } = [];
}

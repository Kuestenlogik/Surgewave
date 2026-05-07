using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Manifest for a Surgewave plugin package (.swpkg).
/// </summary>
[SuppressMessage("Design", "CA1056:URI-like properties should not be strings",
    Justification = "JSON serialization requires string type for URLs")]
public sealed class PluginManifest
{
    [JsonPropertyName("id")]
    public required string Id { get; init; }

    [JsonPropertyName("name")]
    public required string Name { get; init; }

    [JsonPropertyName("version")]
    public required string Version { get; init; }

    [JsonPropertyName("description")]
    public string? Description { get; init; }

    [JsonPropertyName("authors")]
    public string[]? Authors { get; init; }

    [JsonPropertyName("license")]
    public string? License { get; init; }

    [JsonPropertyName("projectUrl")]
    public string? ProjectUrl { get; init; }

    [JsonPropertyName("tags")]
    public string[]? Tags { get; init; }

    /// <summary>
    /// Path to an icon file inside the .swpkg package (e.g., "icon.png").
    /// Supported formats: PNG, SVG. Recommended size: 128x128 or 256x256.
    /// Displayed in Surgewave Control Plugin UI and Marketplace.
    /// </summary>
    [JsonPropertyName("icon")]
    public string? Icon { get; init; }

    [JsonPropertyName("minRuntimeVersion")]
    public string? MinRuntimeVersion { get; init; }

    [JsonPropertyName("dependencies")]
    public Dictionary<string, string>? Dependencies { get; init; }

    [JsonPropertyName("surgewaveDependencies")]
    public PluginDependency[]? SurgewaveDependencies { get; init; }

    [JsonPropertyName("sha256")]
    public string? Sha256 { get; init; }

    /// <summary>
    /// Plugin assemblies to scan for IPlugin implementations.
    /// Other DLLs in lib/ are loaded as dependencies but not scanned.
    /// </summary>
    [JsonPropertyName("assemblies")]
    public required string[] Assemblies { get; init; }

    /// <summary>
    /// Path to the plugin's default settings file inside the .swpkg package
    /// (typically <c>"pluginsettings.json"</c>). When the plugin is installed,
    /// this file is extracted next to the plugin's DLLs and the broker layers
    /// it into its <see cref="Microsoft.Extensions.Configuration.IConfiguration"/>
    /// at a lower priority than the broker's own <c>appsettings.json</c> — so
    /// plugin authors can ship recommended defaults that user overrides still
    /// take precedence over.
    ///
    /// <para>
    /// The file should contain just the plugin's own configuration sections
    /// (e.g. a top-level <c>"Surgewave:Mqtt"</c> object) — not a full broker
    /// appsettings document. Schema: standard JSON, no comments, no trailing
    /// commas (it is read by the same JSON provider as the broker's appsettings).
    /// </para>
    /// </summary>
    [JsonPropertyName("pluginSettings")]
    public string? PluginSettings { get; init; }

    [JsonPropertyName("$schema")]
    public string? Schema { get; init; }
}

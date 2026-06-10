using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Plugins.Packaging.Recommendations;

/// <summary>
/// A plugin that Surgewave Core recommends installing as part of a guided
/// onboarding flow (G18 "AI quickstart" — and any future curated bundles).
/// Recommendations are <b>not</b> bundled into the broker — they remain
/// regular Surgewave Plugin Packages installable via <c>surgewave plugins
/// install</c>. The catalog is just a curated hint list so operators do
/// not have to grep NuGet.
/// </summary>
public sealed class RecommendedPlugin
{
    [JsonPropertyName("id")]
    public required string Id { get; init; }

    [JsonPropertyName("displayName")]
    public required string DisplayName { get; init; }

    [JsonPropertyName("category")]
    public required string Category { get; init; }

    [JsonPropertyName("description")]
    public required string Description { get; init; }

    [JsonPropertyName("whyRecommended")]
    public required string WhyRecommended { get; init; }

    [JsonPropertyName("projectUrl")]
    public string? ProjectUrl { get; init; }

    [JsonPropertyName("source")]
    public string? Source { get; init; }

    [JsonPropertyName("relevantPages")]
    public string[] RelevantPages { get; init; } = [];
}

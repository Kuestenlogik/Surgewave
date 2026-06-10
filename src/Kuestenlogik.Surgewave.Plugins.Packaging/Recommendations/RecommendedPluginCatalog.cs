using System.Reflection;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Plugins.Packaging.Recommendations;

/// <summary>
/// Reads the curated list of plugins Surgewave Core recommends. The catalog
/// ships as an embedded JSON resource so it travels with the CLI and Control
/// binaries without a network round-trip and can be updated by shipping a
/// new Surgewave release.
/// </summary>
public static class RecommendedPluginCatalog
{
    private const string ResourceName =
        "Kuestenlogik.Surgewave.Plugins.Packaging.Recommendations.recommended-plugins.json";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private static IReadOnlyList<RecommendedPlugin>? _cached;

    public static IReadOnlyList<RecommendedPlugin> Load()
    {
        if (_cached is not null) return _cached;

        var assembly = typeof(RecommendedPluginCatalog).Assembly;
        using var stream = assembly.GetManifestResourceStream(ResourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource missing: {ResourceName}. Check the .csproj "
                + "EmbeddedResource include + LogicalName.");

        var payload = JsonSerializer.Deserialize<CatalogPayload>(stream, JsonOptions)
            ?? throw new InvalidOperationException("recommended-plugins.json is empty.");

        _cached = payload.Plugins;
        return _cached;
    }

    public static IReadOnlyList<RecommendedPlugin> ForPage(string pageRoute)
    {
        var route = pageRoute.TrimStart('/');
        return Load()
            .Where(p => p.RelevantPages.Any(rp =>
                rp.TrimStart('/').Equals(route, StringComparison.OrdinalIgnoreCase)))
            .ToList();
    }

    private sealed class CatalogPayload
    {
        public List<RecommendedPlugin> Plugins { get; set; } = [];
    }
}

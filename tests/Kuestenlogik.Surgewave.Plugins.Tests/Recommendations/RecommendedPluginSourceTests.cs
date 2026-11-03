using Kuestenlogik.Surgewave.Plugins.Packaging.Recommendations;
using Kuestenlogik.Surgewave.Plugins.Sources;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Recommendations;

/// <summary>
/// Every recommended plugin names a source a fresh install knows (#137). The AI entry pinned nuget.org, where the
/// plugin was never published, so <c>plugins recommend --apply</c> and the Control banner pointed at nothing.
/// </summary>
public sealed class RecommendedPluginSourceTests
{
    [Fact]
    public void EveryRecommendationNamesADefaultSource()
    {
        var defaults = PluginSourceConfig.CreateDefault().Sources.Select(s => s.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var plugin in RecommendedPluginCatalog.Load())
            Assert.True(plugin.Source is not null && defaults.Contains(plugin.Source),
                $"{plugin.Id} names source '{plugin.Source}', which is not one of the default sources ({string.Join(", ", defaults)})");
    }

    [Fact]
    public void TheAiRecommendationIsTheAiPluginFromItsOwnReleases()
    {
        var ai = Assert.Single(RecommendedPluginCatalog.Load(), p => p.Category == "ai");
        Assert.Equal("Kuestenlogik.Surgewave.AI", ai.Id); // the id in Surgewave.Ai/plugin.json

        var source = Assert.Single(PluginSourceConfig.CreateDefault().Sources, s => s.Name == ai.Source);
        Assert.Equal("github", source.Type);
        Assert.Equal("Kuestenlogik/Surgewave.Ai", source.Url);
    }
}

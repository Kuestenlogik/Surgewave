using Kuestenlogik.Surgewave.Plugins.Marketplace;
using Xunit;

namespace Kuestenlogik.Surgewave.Plugins.Marketplace.Tests;

public sealed class PluginCategoryClassifierTests
{
    [Theory]
    [InlineData("storage-engine", PluginCategory.StorageEngine)]
    [InlineData("storage", PluginCategory.StorageEngine)]
    [InlineData("tiered-storage", PluginCategory.StorageEngine)]
    [InlineData("connector", PluginCategory.Connector)]
    [InlineData("source", PluginCategory.Connector)]
    [InlineData("sink", PluginCategory.Connector)]
    [InlineData("protocol", PluginCategory.Protocol)]
    [InlineData("schema-handler", PluginCategory.SchemaHandler)]
    [InlineData("schema-format", PluginCategory.SchemaHandler)]
    [InlineData("ai", PluginCategory.Ai)]
    [InlineData("llm", PluginCategory.Ai)]
    [InlineData("embedding", PluginCategory.Ai)]
    [InlineData("broker-extension", PluginCategory.BrokerExtension)]
    [InlineData("broker-plugin", PluginCategory.BrokerExtension)]
    public void Each_canonical_tag_maps_to_expected_bucket(string tag, PluginCategory expected)
    {
        Assert.Equal(expected, PluginCategoryClassifier.Classify([tag]));
    }

    [Fact]
    public void Null_or_empty_tags_yield_Other()
    {
        Assert.Equal(PluginCategory.Other, PluginCategoryClassifier.Classify(null));
        Assert.Equal(PluginCategory.Other, PluginCategoryClassifier.Classify([]));
    }

    [Fact]
    public void Unknown_tag_yields_Other()
    {
        Assert.Equal(PluginCategory.Other, PluginCategoryClassifier.Classify([ "foo", "bar" ]));
    }

    [Fact]
    public void First_matching_bucket_wins_when_multiple_tags_qualify()
    {
        // storage-engine is checked before connector — order is documented + stable
        Assert.Equal(PluginCategory.StorageEngine,
            PluginCategoryClassifier.Classify([ "connector", "storage-engine" ]));
    }

    [Fact]
    public void Classifier_is_case_insensitive()
    {
        Assert.Equal(PluginCategory.Protocol, PluginCategoryClassifier.Classify([ "PROTOCOL" ]));
    }
}

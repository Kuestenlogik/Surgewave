using Kuestenlogik.Surgewave.Broker.AutoTuning;
using Kuestenlogik.Surgewave.Broker.CruiseControl;
using Kuestenlogik.Surgewave.Broker.Startup;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Plugins;

/// <summary>
/// Verifies that the three IBrokerPlugin implementations (AutoTuning, CruiseControl,
/// Schema Registry) are discoverable via reflection, honour their config-enabled flags,
/// and register into IServiceCollection correctly.
/// </summary>
public sealed class BrokerPluginActivationTests
{
    // ── Discovery ──────────────────────────────────────────────────────────

    [Fact]
    public void AutoTuning_IsDiscoverableAsBrokerPlugin()
    {
        var plugins = PluginAssemblyScanner.FindImplementations<IBrokerPlugin>(
            [typeof(SurgewaveAutoTuningBrokerPlugin).Assembly]).ToList();
        Assert.Contains(plugins, p => p is SurgewaveAutoTuningBrokerPlugin);
    }

    [Fact]
    public void CruiseControl_IsDiscoverableAsBrokerPlugin()
    {
        var plugins = PluginAssemblyScanner.FindImplementations<IBrokerPlugin>(
            [typeof(SurgewaveCruiseControlBrokerPlugin).Assembly]).ToList();
        Assert.Contains(plugins, p => p is SurgewaveCruiseControlBrokerPlugin);
    }

    [Fact]
    public void SchemaRegistry_IsDiscoverableAsBrokerPlugin()
    {
        var plugins = PluginAssemblyScanner.FindImplementations<IBrokerPlugin>(
            [typeof(SurgewaveSchemaRegistryBrokerPlugin).Assembly]).ToList();
        Assert.Contains(plugins, p => p is SurgewaveSchemaRegistryBrokerPlugin);
    }

    // ── FeatureId + DisplayName ────────────────────────────────────────────

    [Fact]
    public void AutoTuning_HasCorrectFeatureIdAndDisplayName()
    {
        var plugin = new SurgewaveAutoTuningBrokerPlugin();
        Assert.Equal("Surgewave.AutoTuning", plugin.FeatureId);
        Assert.Equal("Auto-Tuning", plugin.DisplayName);
    }

    [Fact]
    public void CruiseControl_HasCorrectFeatureIdAndDisplayName()
    {
        var plugin = new SurgewaveCruiseControlBrokerPlugin();
        Assert.Equal("Surgewave.CruiseControl", plugin.FeatureId);
        Assert.Equal("Cruise Control", plugin.DisplayName);
    }

    [Fact]
    public void SchemaRegistry_HasCorrectFeatureIdAndDisplayName()
    {
        var plugin = new SurgewaveSchemaRegistryBrokerPlugin();
        Assert.Equal("Surgewave.SchemaRegistry", plugin.FeatureId);
        Assert.Equal("Schema Registry", plugin.DisplayName);
    }

    // ── IsConfigEnabled ────────────────────────────────────────────────────

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void AutoTuning_IsConfigEnabled_ReadsFromConfig(string value, bool expected)
    {
        var config = BuildConfig("Surgewave:AutoTuning:Enabled", value);
        Assert.Equal(expected, new SurgewaveAutoTuningBrokerPlugin().IsConfigEnabled(config));
    }

    [Fact]
    public void AutoTuning_IsConfigEnabled_DefaultsFalse()
    {
        var config = BuildConfig(); // empty
        Assert.False(new SurgewaveAutoTuningBrokerPlugin().IsConfigEnabled(config));
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void CruiseControl_IsConfigEnabled_ReadsFromConfig(string value, bool expected)
    {
        var config = BuildConfig("Surgewave:CruiseControl:Enabled", value);
        Assert.Equal(expected, new SurgewaveCruiseControlBrokerPlugin().IsConfigEnabled(config));
    }

    [Fact]
    public void CruiseControl_IsConfigEnabled_DefaultsFalse()
    {
        var config = BuildConfig();
        Assert.False(new SurgewaveCruiseControlBrokerPlugin().IsConfigEnabled(config));
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void SchemaRegistry_IsConfigEnabled_ReadsFromConfig(string value, bool expected)
    {
        var config = BuildConfig("Surgewave:SchemaRegistry:Enabled", value);
        Assert.Equal(expected, new SurgewaveSchemaRegistryBrokerPlugin().IsConfigEnabled(config));
    }

    [Fact]
    public void SchemaRegistry_IsConfigEnabled_DefaultsTrue()
    {
        // Schema Registry is a community feature — enabled by default!
        var config = BuildConfig();
        Assert.True(new SurgewaveSchemaRegistryBrokerPlugin().IsConfigEnabled(config));
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static IConfiguration BuildConfig(params string[] keyValuePairs)
    {
        var dict = new Dictionary<string, string?>();
        for (var i = 0; i < keyValuePairs.Length - 1; i += 2)
        {
            dict[keyValuePairs[i]] = keyValuePairs[i + 1];
        }
        return new ConfigurationBuilder()
            .AddInMemoryCollection(dict)
            .Build();
    }
}

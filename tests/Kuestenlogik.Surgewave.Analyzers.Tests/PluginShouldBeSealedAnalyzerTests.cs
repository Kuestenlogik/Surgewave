using Microsoft.CodeAnalysis;
using Xunit;

namespace Kuestenlogik.Surgewave.Analyzers.Tests;

public sealed class PluginShouldBeSealedAnalyzerTests
{
    [Fact]
    public void Fires_on_unsealed_broker_plugin()
    {
        const string source = """
            using Kuestenlogik.Surgewave.Plugins;
            using Microsoft.Extensions.Configuration;
            using Microsoft.Extensions.DependencyInjection;

            public class MyBroker : IBrokerPlugin
            {
                public string FeatureId => "X";
                public string DisplayName => "X";
                public bool IsConfigEnabled(IConfiguration c) => true;
                public void ConfigureServices(IServiceCollection s, IConfiguration c) { }
            }
            """;

        var diags = AnalyzerHarness.Run(new PluginShouldBeSealedAnalyzer(), source);

        Assert.Contains(diags, d => d.Id == "SRWV001" && d.Severity == DiagnosticSeverity.Warning);
    }

    [Fact]
    public void Stays_quiet_on_sealed_broker_plugin()
    {
        const string source = """
            using Kuestenlogik.Surgewave.Plugins;
            using Microsoft.Extensions.Configuration;
            using Microsoft.Extensions.DependencyInjection;

            public sealed class MyBroker : IBrokerPlugin
            {
                public string FeatureId => "X";
                public string DisplayName => "X";
                public bool IsConfigEnabled(IConfiguration c) => true;
                public void ConfigureServices(IServiceCollection s, IConfiguration c) { }
            }
            """;

        var diags = AnalyzerHarness.Run(new PluginShouldBeSealedAnalyzer(), source);

        Assert.DoesNotContain(diags, d => d.Id == "SRWV001");
    }

    [Fact]
    public void Stays_quiet_on_non_plugin_class()
    {
        const string source = """
            public class JustAClass { }
            """;

        var diags = AnalyzerHarness.Run(new PluginShouldBeSealedAnalyzer(), source);

        Assert.DoesNotContain(diags, d => d.Id == "SRWV001");
    }
}

using Microsoft.CodeAnalysis;
using Xunit;

namespace Kuestenlogik.Surgewave.Analyzers.Tests;

public sealed class ParameterlessCtorRequiredAnalyzerTests
{
    [Fact]
    public void Fires_when_only_param_ctor_declared()
    {
        const string source = """
            using Kuestenlogik.Surgewave.Plugins;
            using Microsoft.Extensions.Configuration;
            using Microsoft.Extensions.DependencyInjection;

            public sealed class MyBroker : IBrokerPlugin
            {
                public MyBroker(string mandatory) { }
                public string FeatureId => "X";
                public string DisplayName => "X";
                public bool IsConfigEnabled(IConfiguration c) => true;
                public void ConfigureServices(IServiceCollection s, IConfiguration c) { }
            }
            """;

        var diags = AnalyzerHarness.Run(new ParameterlessCtorRequiredAnalyzer(), source);

        Assert.Contains(diags, d => d.Id == "SRWV004" && d.Severity == DiagnosticSeverity.Error);
    }

    [Fact]
    public void Stays_quiet_when_default_compiler_ctor_present()
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

        var diags = AnalyzerHarness.Run(new ParameterlessCtorRequiredAnalyzer(), source);

        Assert.DoesNotContain(diags, d => d.Id == "SRWV004");
    }

    [Fact]
    public void Stays_quiet_when_both_ctors_exist()
    {
        const string source = """
            using Kuestenlogik.Surgewave.Plugins;
            using Microsoft.Extensions.Configuration;
            using Microsoft.Extensions.DependencyInjection;

            public sealed class MyBroker : IBrokerPlugin
            {
                public MyBroker() { }
                public MyBroker(string optional) : this() { }
                public string FeatureId => "X";
                public string DisplayName => "X";
                public bool IsConfigEnabled(IConfiguration c) => true;
                public void ConfigureServices(IServiceCollection s, IConfiguration c) { }
            }
            """;

        var diags = AnalyzerHarness.Run(new ParameterlessCtorRequiredAnalyzer(), source);

        Assert.DoesNotContain(diags, d => d.Id == "SRWV004");
    }
}

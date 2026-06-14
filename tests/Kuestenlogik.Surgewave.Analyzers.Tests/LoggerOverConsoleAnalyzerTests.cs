using Xunit;

namespace Kuestenlogik.Surgewave.Analyzers.Tests;

public sealed class LoggerOverConsoleAnalyzerTests
{
    [Fact]
    public void Fires_on_ConsoleWriteLine_inside_plugin()
    {
        const string source = """
            using System;
            using Kuestenlogik.Surgewave.Plugins;
            using Microsoft.Extensions.Configuration;
            using Microsoft.Extensions.DependencyInjection;

            public sealed class MyBroker : IBrokerPlugin
            {
                public string FeatureId => "X";
                public string DisplayName => "X";
                public bool IsConfigEnabled(IConfiguration c) => true;
                public void ConfigureServices(IServiceCollection s, IConfiguration c)
                {
                    Console.WriteLine("hello");
                }
            }
            """;

        var diags = AnalyzerHarness.Run(new LoggerOverConsoleAnalyzer(), source);

        Assert.Contains(diags, d => d.Id == "SRWV010");
    }

    [Fact]
    public void Fires_on_ConsoleWrite_too()
    {
        const string source = """
            using System;
            using Kuestenlogik.Surgewave.Plugins;
            using Microsoft.Extensions.Configuration;
            using Microsoft.Extensions.DependencyInjection;

            public sealed class MyBroker : IBrokerPlugin
            {
                public string FeatureId => "X";
                public string DisplayName => "X";
                public bool IsConfigEnabled(IConfiguration c) => true;
                public void ConfigureServices(IServiceCollection s, IConfiguration c)
                {
                    Console.Write("hi");
                }
            }
            """;

        var diags = AnalyzerHarness.Run(new LoggerOverConsoleAnalyzer(), source);

        Assert.Contains(diags, d => d.Id == "SRWV010");
    }

    [Fact]
    public void Stays_quiet_when_Console_used_outside_plugin_class()
    {
        const string source = """
            using System;

            public static class JustAUtility
            {
                public static void Print() => Console.WriteLine("hi");
            }
            """;

        var diags = AnalyzerHarness.Run(new LoggerOverConsoleAnalyzer(), source);

        Assert.DoesNotContain(diags, d => d.Id == "SRWV010");
    }
}

using Kuestenlogik.Surgewave.Cli.Commands.Pipelines;
using Kuestenlogik.Surgewave.Pipelines;
using Xunit;

namespace Kuestenlogik.Surgewave.Tool.Tests.Commands.Pipelines;

/// <summary>
/// Verifies that <see cref="PipelineAssemblyScanner"/> discovers ISurgewavePipeline
/// implementations across an AssemblyLoadContext boundary — the test assembly itself
/// plays the role of the user's pipeline library.
/// </summary>
public class PipelineAssemblyScannerTests
{
    private sealed record OrderEvent(double Amount);

    public sealed class ScannerProbePipeline : ISurgewavePipeline
    {
        public BuiltPipeline Define() => Pipeline
            .From<OrderEvent>("orders")
            .Named("scanner-probe")
            .Filter(o => o.Amount > 10)
            .To("probed")
            .Build();
    }

    public sealed class UnnamedProbePipeline : ISurgewavePipeline
    {
        public BuiltPipeline Define() => Pipeline
            .From<OrderEvent>("orders")
            .Filter(o => o.Amount > 0)
            .To("unnamed-probed")
            .Build();
    }

    [Fact]
    public void Scan_FindsPipelinesInAssembly()
    {
        var assemblyPath = typeof(PipelineAssemblyScannerTests).Assembly.Location;

        var scanned = PipelineAssemblyScanner.Scan(assemblyPath);

        var named = scanned.Single(p => p.Pipeline.Name == "scanner-probe");
        Assert.EndsWith(nameof(ScannerProbePipeline), named.TypeName);
        var node = Assert.Single(named.Pipeline.Nodes);
        Assert.Equal("orders", node.Config["topics"]);
        Assert.Equal("probed", node.Config["output.topic"]);
    }

    [Fact]
    public void Scan_AppliesKebabCaseFallbackName()
    {
        var assemblyPath = typeof(PipelineAssemblyScannerTests).Assembly.Location;

        var scanned = PipelineAssemblyScanner.Scan(assemblyPath);

        Assert.Contains(scanned, p => p.Pipeline.Name == "unnamed-probe-pipeline");
    }

    [Fact]
    public void Scan_MissingAssembly_Throws()
    {
        Assert.Throws<FileNotFoundException>(
            () => PipelineAssemblyScanner.Scan(Path.Combine(Path.GetTempPath(), "does-not-exist.dll")));
    }
}

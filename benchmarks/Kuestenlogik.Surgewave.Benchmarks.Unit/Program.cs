using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

public static class BenchmarkMain
{
    public static void Main(string[] args)
    {
        var config = DefaultConfig.Instance
            .WithArtifactsPath(Path.Combine(FindRepoRoot(), "artifacts", "benchmarks"));

        BenchmarkSwitcher.FromAssembly(typeof(BenchmarkMain).Assembly).Run(args, config);
    }

    private static string FindRepoRoot()
    {
        var dir = Directory.GetCurrentDirectory();
        while (dir != null && !File.Exists(Path.Combine(dir, "Kuestenlogik.Surgewave.slnx")))
            dir = Directory.GetParent(dir)?.FullName;
        return dir ?? Directory.GetCurrentDirectory();
    }
}

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

public static class BenchmarkMain
{
    public static void Main(string[] args)
    {
        var config = DefaultConfig.Instance
            .WithArtifactsPath(Path.Combine(FindRepoRoot(), "artifacts", "benchmarks"))
            // BenchmarkDotNet builds a generated boilerplate project that references this one, and
            // this one transitively references most of the solution. Its default 120 s build budget
            // is not enough for that closure, so every local run died with an empty "Build Error"
            // and the misleading "failed to build the auto-generated boilerplate code" — the real
            // message, "The configured timeout 00:02:00 was reached!", only appears in the log file.
            //
            // The build is also doing more than it needs to: Directory.Build.targets sets
            // GeneratePackageOnBuild, so the boilerplate build packs a .nupkg for every packable
            // dependency it compiles. Raising the budget makes the run work; not packing during it
            // would make it fast.
            .WithBuildTimeout(TimeSpan.FromMinutes(10));

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

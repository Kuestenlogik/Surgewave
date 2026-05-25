using BenchmarkDotNet.Running;

namespace Kuestenlogik.Surgewave.Benchmarks.Storage;

public static class Program
{
    public static void Main(string[] args)
    {
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}

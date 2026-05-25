namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Loads all saved JSON benchmark results from
/// <c>artifacts/benchmarks/results/</c> and prints a comparison table.
///
/// Run:
///   dotnet run -- compare
/// </summary>
public static class CompareCommand
{
    public static Task RunAsync(string[] args)
    {
        Console.WriteLine("Loading saved benchmark results...");
        Console.WriteLine($"  Directory: {BenchmarkResultStore.ResultsDirectory}");
        Console.WriteLine();

        var results = BenchmarkResultStore.LoadAll();

        if (results.Count == 0)
        {
            Console.WriteLine("No results found. Run one or more benchmark commands first:");
            Console.WriteLine("  dotnet run -- benchmark-surgewave");
            Console.WriteLine("  dotnet run -- benchmark-kafka   [bootstrap]");
            Console.WriteLine("  dotnet run -- benchmark-redpanda [bootstrap]");
            Console.WriteLine("  dotnet run -- benchmark-pulsar  [bootstrap]");
            Console.WriteLine("  dotnet run -- benchmark-nats    [natsUrl]");
            return Task.CompletedTask;
        }

        BenchmarkResultStore.PrintComparisonTable(results);

        return Task.CompletedTask;
    }
}

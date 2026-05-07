using Kuestenlogik.Surgewave.Benchmarks.RealWorld.Scenarios;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld;

/// <summary>
/// Orchestrates benchmark scenario execution, collecting results and producing reports.
/// Supports running individual scenarios or the full suite.
/// </summary>
public sealed class BenchmarkRunner
{
    private readonly BenchmarkConfig _config;
    private readonly List<BenchmarkResult> _results = [];

    public BenchmarkRunner(BenchmarkConfig config)
    {
        _config = config;
    }

    /// <summary>All collected results after running scenarios.</summary>
    public IReadOnlyList<BenchmarkResult> Results => _results;

    /// <summary>
    /// Runs the specified scenario by name.
    /// Valid names: throughput, latency, scaling, replication, consumer, failover, storage, all.
    /// </summary>
    public async Task RunAsync(string scenario)
    {
        AnsiConsole.WriteLine();
        var header = new Rule("[bold white on blue] Surgewave Real-World Benchmark Suite [/]");
        header.Justification = Justify.Center;
        AnsiConsole.Write(header);
        AnsiConsole.WriteLine();

        AnsiConsole.MarkupLine($"[dim]Brokers: {_config.BrokerCount} | Messages: {_config.MessageCount:N0} | Size: {_config.MessageSizeBytes}B | Max duration: {_config.DurationSeconds}s[/]");
        AnsiConsole.WriteLine();

        switch (scenario.ToLowerInvariant())
        {
            case "throughput":
                _results.Add(await new ThroughputScenario(_config).RunAsync());
                break;

            case "latency":
                _results.Add(await new LatencyScenario(_config).RunAsync());
                break;

            case "scaling":
                _results.Add(await new ScalingScenario(_config).RunAsync());
                break;

            case "replication":
                _results.Add(await new ReplicationScenario(_config).RunAsync());
                break;

            case "consumer":
                _results.Add(await new ConsumerScenario(_config).RunAsync());
                break;

            case "failover":
                _results.Add(await new FailoverScenario(_config).RunAsync());
                break;

            case "storage":
                _results.Add(await new StorageScenario(_config).RunAsync());
                break;

            case "all":
                await RunAllAsync();
                break;

            default:
                AnsiConsole.MarkupLine($"[red]Unknown scenario: {scenario}[/]");
                AnsiConsole.MarkupLine("[dim]Available: throughput, latency, scaling, replication, consumer, failover, storage, all[/]");
                return;
        }

        // Print results
        BenchmarkReport.PrintToConsole(_results);

        // Compare with baseline if provided
        if (!string.IsNullOrEmpty(_config.ComparePath) && File.Exists(_config.ComparePath))
        {
            var baseline = BenchmarkReport.LoadJson(_config.ComparePath);
            BenchmarkReport.PrintComparison(_results, baseline);
        }

        // Save JSON output
        if (!string.IsNullOrEmpty(_config.OutputPath))
        {
            BenchmarkReport.SaveJson(_config.OutputPath, _results);
            AnsiConsole.MarkupLine($"[dim]Results saved to: {_config.OutputPath}[/]");
        }

        // Generate markdown report
        if (!string.IsNullOrEmpty(_config.ReportPath))
        {
            var markdown = BenchmarkReport.GenerateMarkdown(_results);
            var directory = Path.GetDirectoryName(_config.ReportPath);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);
            await File.WriteAllTextAsync(_config.ReportPath, markdown);
            AnsiConsole.MarkupLine($"[dim]Report saved to: {_config.ReportPath}[/]");
        }
    }

    private async Task RunAllAsync()
    {
        var scenarios = new (string Name, Func<Task<BenchmarkResult>> Run)[]
        {
            ("throughput", () => new ThroughputScenario(_config).RunAsync()),
            ("latency", () => new LatencyScenario(_config).RunAsync()),
            ("scaling", () => new ScalingScenario(_config).RunAsync()),
            ("replication", () => new ReplicationScenario(_config).RunAsync()),
            ("consumer", () => new ConsumerScenario(_config).RunAsync()),
            ("failover", () => new FailoverScenario(_config).RunAsync()),
            ("storage", () => new StorageScenario(_config).RunAsync())
        };

        foreach (var (name, run) in scenarios)
        {
            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Rule($"[bold]{name}[/]"));
            AnsiConsole.WriteLine();

            try
            {
                _results.Add(await run());
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Scenario '{name}' failed: {ex.Message}[/]");
            }
        }
    }
}

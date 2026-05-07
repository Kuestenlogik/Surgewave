using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld;

/// <summary>
/// Generates benchmark reports in multiple formats: console (Spectre.Console tables),
/// Markdown, and JSON. Supports regression comparison against a saved baseline.
/// </summary>
public static class BenchmarkReport
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() }
    };

    /// <summary>
    /// Prints all results as rich Spectre.Console tables.
    /// </summary>
    public static void PrintToConsole(IReadOnlyList<BenchmarkResult> results)
    {
        AnsiConsole.WriteLine();
        var rule = new Rule("[bold cyan]Surgewave Real-World Benchmark Results[/]");
        rule.Justification = Justify.Center;
        AnsiConsole.Write(rule);
        AnsiConsole.WriteLine();

        foreach (var result in results)
        {
            PrintScenarioResult(result);
        }

        // Summary table
        var summaryTable = new Table()
            .Border(TableBorder.Rounded)
            .Title("[bold]Summary[/]")
            .AddColumn("Scenario")
            .AddColumn("Duration")
            .AddColumn("Key Metric");

        foreach (var result in results)
        {
            var keyMetric = GetKeyMetric(result);
            summaryTable.AddRow(
                $"[bold]{result.Scenario}[/]",
                result.Duration.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture) + "s",
                keyMetric);
        }

        AnsiConsole.Write(summaryTable);
        AnsiConsole.WriteLine();
    }

    /// <summary>
    /// Prints results compared against a baseline, highlighting regressions and improvements.
    /// </summary>
    public static void PrintComparison(IReadOnlyList<BenchmarkResult> current, IReadOnlyList<BenchmarkResult> baseline)
    {
        AnsiConsole.WriteLine();
        var rule = new Rule("[bold yellow]Regression Comparison[/]");
        rule.Justification = Justify.Center;
        AnsiConsole.Write(rule);
        AnsiConsole.WriteLine();

        var table = new Table()
            .Border(TableBorder.Rounded)
            .AddColumn("Scenario")
            .AddColumn("Metric")
            .AddColumn("Baseline")
            .AddColumn("Current")
            .AddColumn("Delta");

        foreach (var result in current)
        {
            var baselineResult = baseline.FirstOrDefault(b =>
                string.Equals(b.Scenario, result.Scenario, StringComparison.OrdinalIgnoreCase));
            if (baselineResult == null)
                continue;

            foreach (var (key, value) in result.Metrics)
            {
                if (!baselineResult.Metrics.TryGetValue(key, out var baselineValue) || baselineValue == 0)
                    continue;

                var delta = (value - baselineValue) / baselineValue * 100;
                var deltaStr = delta >= 0 ? $"[green]+{delta:F1}%[/]" : $"[red]{delta:F1}%[/]";

                // Invert delta color for latency metrics (lower is better)
                if (key.Contains("_ms", StringComparison.OrdinalIgnoreCase) ||
                    key.Contains("latency", StringComparison.OrdinalIgnoreCase))
                {
                    deltaStr = delta <= 0 ? $"[green]{delta:F1}%[/]" : $"[red]+{delta:F1}%[/]";
                }

                table.AddRow(
                    result.Scenario,
                    key,
                    baselineValue.ToString("N2", CultureInfo.InvariantCulture),
                    value.ToString("N2", CultureInfo.InvariantCulture),
                    deltaStr);
            }
        }

        AnsiConsole.Write(table);
        AnsiConsole.WriteLine();
    }

    /// <summary>
    /// Generates a Markdown report string.
    /// </summary>
    public static string GenerateMarkdown(IReadOnlyList<BenchmarkResult> results)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# Surgewave Real-World Benchmark Report");
        sb.AppendLine();

        if (results.Count > 0)
        {
            var env = results[0].Environment;
            sb.AppendLine("## Environment");
            sb.AppendLine();
            sb.AppendLine($"- **OS:** {env.Os}");
            sb.AppendLine($"- **Processors:** {env.ProcessorCount}");
            sb.AppendLine($"- **Runtime:** {env.Runtime}");
            sb.AppendLine($"- **Machine:** {env.MachineName}");
            sb.AppendLine($"- **Date:** {results[0].Timestamp:yyyy-MM-dd HH:mm:ss UTC}");
            sb.AppendLine();
        }

        sb.AppendLine("## Results");
        sb.AppendLine();

        foreach (var result in results)
        {
            sb.AppendLine($"### {result.Scenario}");
            sb.AppendLine();
            sb.AppendLine($"_{result.Description}_ (Duration: {result.Duration.TotalSeconds:F1}s)");
            sb.AppendLine();
            sb.AppendLine("| Metric | Value |");
            sb.AppendLine("|--------|-------|");

            foreach (var (key, value) in result.Metrics.OrderBy(m => m.Key, StringComparer.Ordinal))
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"| {key} | {value:N2} |");
            }

            sb.AppendLine();
        }

        return sb.ToString();
    }

    /// <summary>
    /// Saves results as JSON for regression comparison.
    /// </summary>
    public static void SaveJson(string path, IReadOnlyList<BenchmarkResult> results)
    {
        var json = JsonSerializer.Serialize(results, s_jsonOptions);
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);
        File.WriteAllText(path, json);
    }

    /// <summary>
    /// Loads results from a previously saved JSON file.
    /// </summary>
    public static IReadOnlyList<BenchmarkResult> LoadJson(string path)
    {
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<List<BenchmarkResult>>(json, s_jsonOptions) ?? [];
    }

    private static void PrintScenarioResult(BenchmarkResult result)
    {
        var table = new Table()
            .Border(TableBorder.Rounded)
            .Title($"[bold]{result.Scenario}[/] - {result.Description}")
            .AddColumn("Metric")
            .AddColumn("Value");

        foreach (var (key, value) in result.Metrics.OrderBy(m => m.Key, StringComparer.Ordinal))
        {
            table.AddRow(key, value.ToString("N2", CultureInfo.InvariantCulture));
        }

        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine($"  [dim]Duration: {result.Duration.TotalSeconds:F1}s[/]");
        AnsiConsole.WriteLine();
    }

    private static string GetKeyMetric(BenchmarkResult result)
    {
        if (result.Metrics.TryGetValue("throughput_msg_sec", out var throughput))
            return $"{throughput:N0} msg/sec";
        if (result.Metrics.TryGetValue("produce_throughput_msg_sec", out var prodThroughput))
            return $"{prodThroughput:N0} msg/sec";
        if (result.Metrics.TryGetValue("e2e_p99_ms", out var p99))
            return $"P99: {p99:F2} ms";
        if (result.Metrics.TryGetValue("produce_p99_ms", out var prodP99))
            return $"P99: {prodP99:F2} ms";
        if (result.Metrics.Count > 0)
            return $"{result.Metrics.First().Key}: {result.Metrics.First().Value:N2}";
        return "-";
    }
}

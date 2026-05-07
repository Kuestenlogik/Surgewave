using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Reporting;

/// <summary>
/// Generates comparison reports in multiple formats: Spectre.Console tables, Markdown, and JSON.
/// Supports multi-platform comparisons with configurable baselines.
/// </summary>
public static class ComparisonReportGenerator
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() }
    };

    /// <summary>
    /// Prints all comparison reports as rich Spectre.Console tables.
    /// </summary>
    public static void PrintToConsole(IReadOnlyList<ComparisonReport> reports)
    {
        AnsiConsole.WriteLine();
        AnsiConsole.Write(new Rule("[bold white on blue] Multi-Platform Comparison [/]") { Justification = Justify.Center });
        AnsiConsole.WriteLine();

        foreach (var report in reports)
        {
            PrintReportTable(report);
        }

        // Print summary
        PrintSummary(reports);
    }

    /// <summary>
    /// Generates a Markdown report with multi-platform tables and delta percentages.
    /// </summary>
    public static string GenerateMarkdown(IReadOnlyList<ComparisonReport> reports)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# Multi-Platform Comparison Report");
        sb.AppendLine();
        sb.AppendLine("## Environment");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"- **OS:** {Environment.OSVersion}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- **Processors:** {Environment.ProcessorCount}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- **Runtime:** {RuntimeInformation.FrameworkDescription}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- **Date:** {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        sb.AppendLine();

        foreach (var report in reports)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"## {report.ScenarioName}");
            sb.AppendLine();
            sb.AppendLine(CultureInfo.InvariantCulture, $"_{report.Description}_");
            sb.AppendLine();

            if (report.SubResults is { Count: > 0 })
            {
                AppendSubResultsMarkdown(sb, report);
            }
            else
            {
                AppendMultiPlatformMarkdown(sb, report);
            }

            sb.AppendLine();
        }

        return sb.ToString();
    }

    /// <summary>
    /// Saves comparison reports as JSON for CI integration.
    /// </summary>
    public static void SaveJson(string path, IReadOnlyList<ComparisonReport> reports)
    {
        var json = JsonSerializer.Serialize(reports, s_jsonOptions);
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);
        File.WriteAllText(path, json);
    }

    // ─── Console rendering ──────────────────────────────────────────────

    private static void PrintReportTable(ComparisonReport report)
    {
        if (report.SubResults is { Count: > 0 })
        {
            PrintSubResultsTable(report);
            return;
        }

        var baseline = FindBaseline(report.Results);

        var table = new Table()
            .Border(TableBorder.Rounded)
            .Title($"[bold]{report.ScenarioName}[/] - {report.Description}")
            .AddColumn("Platform")
            .AddColumn(new TableColumn("Produce msg/s").RightAligned())
            .AddColumn(new TableColumn("Consume msg/s").RightAligned())
            .AddColumn(new TableColumn("P99 (ms)").RightAligned())
            .AddColumn(new TableColumn("vs Baseline").RightAligned());

        foreach (var result in report.Results.OrderByDescending(r => r.ProduceThroughputMsgPerSec))
        {
            var isBaseline = baseline != null && result.PlatformType == baseline.PlatformType;
            var delta = FormatDeltaVsBaseline(result, baseline);

            var platformLabel = isBaseline
                ? $"[{result.PlatformType.Color()}]{result.Platform}[/] [dim](baseline)[/]"
                : $"[{result.PlatformType.Color()}]{result.Platform}[/]";

            var p99Str = result.ProduceLatencyP99Ms > 0
                ? result.ProduceLatencyP99Ms.ToString("F2", CultureInfo.InvariantCulture)
                : "[dim]--[/]";

            table.AddRow(
                platformLabel,
                result.ProduceThroughputMsgPerSec.ToString("N0", CultureInfo.InvariantCulture),
                result.ConsumeThroughputMsgPerSec > 0
                    ? result.ConsumeThroughputMsgPerSec.ToString("N0", CultureInfo.InvariantCulture)
                    : "[dim]--[/]",
                p99Str,
                delta);
        }

        AnsiConsole.Write(table);
        AnsiConsole.WriteLine();
    }

    private static void PrintSubResultsTable(ComparisonReport report)
    {
        // Collect all platforms that appear in any sub-result
        var platforms = report.SubResults!
            .SelectMany(sr => sr.Results.Select(r => r.PlatformType))
            .Distinct()
            .OrderBy(p => (int)p)
            .ToList();

        var table = new Table()
            .Border(TableBorder.Rounded)
            .Title($"[bold]{report.ScenarioName}[/] - {report.Description}")
            .AddColumn("Parameter");

        foreach (var platform in platforms)
        {
            table.AddColumn(new TableColumn($"[{platform.Color()}]{platform.DisplayName()}[/]").RightAligned());
        }

        foreach (var sub in report.SubResults!)
        {
            var values = new List<string> { sub.Label };
            foreach (var platform in platforms)
            {
                var result = sub.Results.Find(r => r.PlatformType == platform);
                values.Add(result != null
                    ? result.ProduceThroughputMsgPerSec.ToString("N0", CultureInfo.InvariantCulture)
                    : "[dim]N/A[/]");
            }
            table.AddRow(values.ToArray());
        }

        AnsiConsole.Write(table);
        AnsiConsole.WriteLine();
    }

    private static void PrintSummary(IReadOnlyList<ComparisonReport> reports)
    {
        // Collect all platforms across all reports
        var allPlatforms = reports
            .SelectMany(r => r.Results.Select(res => res.PlatformType))
            .Distinct()
            .OrderBy(p => (int)p)
            .ToList();

        var summaryTable = new Table()
            .Border(TableBorder.Rounded)
            .Title("[bold]Overall Summary[/]")
            .AddColumn("Scenario");

        foreach (var platform in allPlatforms)
        {
            summaryTable.AddColumn(new TableColumn($"[{platform.Color()}]{platform.DisplayName()}[/]").RightAligned());
        }

        foreach (var report in reports)
        {
            var values = new List<string> { $"[bold]{report.ScenarioName}[/]" };

            foreach (var platform in allPlatforms)
            {
                var result = report.GetResult(platform);
                values.Add(result != null
                    ? result.ProduceThroughputMsgPerSec.ToString("N0", CultureInfo.InvariantCulture) + " msg/s"
                    : "[dim]--[/]");
            }

            summaryTable.AddRow(values.ToArray());
        }

        AnsiConsole.Write(summaryTable);
        AnsiConsole.WriteLine();
    }

    // ─── Markdown rendering ──────────────────────────────────────────────

    private static void AppendMultiPlatformMarkdown(StringBuilder sb, ComparisonReport report)
    {
        var baseline = FindBaseline(report.Results);
        var hasLatency = report.Results.Any(r => r.ProduceLatencyP99Ms > 0);

        sb.AppendLine("| Platform | Produce msg/s | Consume msg/s | P99 (ms) | vs Baseline |");
        sb.AppendLine("|----------|---------------|---------------|----------|-------------|");

        foreach (var result in report.Results.OrderByDescending(r => r.ProduceThroughputMsgPerSec))
        {
            var isBaseline = baseline != null && result.PlatformType == baseline.PlatformType;
            var delta = isBaseline ? "baseline" : FormatDeltaVsBaselineMarkdown(result, baseline);

            var p99Str = result.ProduceLatencyP99Ms > 0
                ? result.ProduceLatencyP99Ms.ToString("F2", CultureInfo.InvariantCulture)
                : "--";

            var consumeStr = result.ConsumeThroughputMsgPerSec > 0
                ? result.ConsumeThroughputMsgPerSec.ToString("N0", CultureInfo.InvariantCulture)
                : "--";

            sb.AppendLine(CultureInfo.InvariantCulture,
                $"| {result.Platform} | {result.ProduceThroughputMsgPerSec.ToString("N0", CultureInfo.InvariantCulture)} | {consumeStr} | {p99Str} | {delta} |");
        }
    }

    private static void AppendSubResultsMarkdown(StringBuilder sb, ComparisonReport report)
    {
        var platforms = report.SubResults!
            .SelectMany(sr => sr.Results.Select(r => r.PlatformType))
            .Distinct()
            .OrderBy(p => (int)p)
            .ToList();

        // Header
        sb.Append("| Parameter |");
        foreach (var platform in platforms)
            sb.Append(CultureInfo.InvariantCulture, $" {platform.DisplayName()} |");
        sb.AppendLine();

        sb.Append("|-----------|");
        foreach (var _ in platforms)
            sb.Append("---------------|");
        sb.AppendLine();

        // Rows
        foreach (var sub in report.SubResults!)
        {
            sb.Append(CultureInfo.InvariantCulture, $"| {sub.Label} |");
            foreach (var platform in platforms)
            {
                var result = sub.Results.Find(r => r.PlatformType == platform);
                var val = result != null
                    ? result.ProduceThroughputMsgPerSec.ToString("N0", CultureInfo.InvariantCulture)
                    : "N/A";
                sb.Append(CultureInfo.InvariantCulture, $" {val} |");
            }
            sb.AppendLine();
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────

    /// <summary>
    /// Determines the baseline platform from results. Prefers ApacheKafkaContainer, then Redpanda, then last.
    /// </summary>
    private static ComparisonResult? FindBaseline(List<ComparisonResult> results)
    {
        if (results.Count <= 1) return null;

        return results.Find(r => r.PlatformType == BenchmarkPlatform.ApacheKafkaContainer)
            ?? results.Find(r => r.PlatformType == BenchmarkPlatform.RedpandaContainer)
            ?? results[^1];
    }

    private static string FormatDeltaVsBaseline(ComparisonResult result, ComparisonResult? baseline)
    {
        if (baseline == null || baseline.PlatformType == result.PlatformType)
            return "[dim]baseline[/]";

        if (baseline.ProduceThroughputMsgPerSec == 0)
            return "[dim]--[/]";

        var pct = (result.ProduceThroughputMsgPerSec - baseline.ProduceThroughputMsgPerSec)
            / baseline.ProduceThroughputMsgPerSec * 100;

        return pct >= 0
            ? $"[green]+{pct:F1}%[/]"
            : $"[red]{pct:F1}%[/]";
    }

    private static string FormatDeltaVsBaselineMarkdown(ComparisonResult result, ComparisonResult? baseline)
    {
        if (baseline == null || baseline.PlatformType == result.PlatformType)
            return "baseline";

        if (baseline.ProduceThroughputMsgPerSec == 0)
            return "--";

        var pct = (result.ProduceThroughputMsgPerSec - baseline.ProduceThroughputMsgPerSec)
            / baseline.ProduceThroughputMsgPerSec * 100;

        return $"{pct:+0.0;-0.0}%";
    }
}

using System.Globalization;
using System.Text;
using Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

namespace Kuestenlogik.Surgewave.Benchmarks.Regression;

/// <summary>
/// Generates Markdown reports from regression detection results.
/// </summary>
public sealed class RegressionReportGenerator
{
    /// <summary>
    /// Generates a complete Markdown regression report.
    /// </summary>
    public static string GenerateMarkdown(
        List<RegressionResult> results,
        Dictionary<string, BenchmarkEntry> baseline,
        Dictionary<string, BenchmarkEntry> current)
    {
        var sb = new StringBuilder();

        var regressions = results.Where(r => r.Severity == RegressionSeverity.Regression).ToList();
        var improvements = results.Where(r => r.Severity == RegressionSeverity.Improvement).ToList();
        var newBenchmarks = results.Where(r => r.Severity == RegressionSeverity.New).ToList();
        var stableCount = current.Count - regressions.Select(r => r.BenchmarkName).Distinct().Count()
                          - improvements.Select(r => r.BenchmarkName).Distinct().Count()
                          - newBenchmarks.Count;

        var status = regressions.Count > 0 ? "FAIL" : "PASS";

        sb.AppendLine("## Performance Regression Report");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"**Status:** {status}");
        sb.AppendLine(CultureInfo.InvariantCulture,
            $"**Benchmarks:** {current.Count} total | " +
            $"{regressions.Select(r => r.BenchmarkName).Distinct().Count()} regressions | " +
            $"{improvements.Select(r => r.BenchmarkName).Distinct().Count()} improvements | " +
            $"{Math.Max(0, stableCount)} stable | " +
            $"{newBenchmarks.Count} new");
        sb.AppendLine();

        // Regressions section
        if (regressions.Count > 0)
        {
            sb.AppendLine("### Regressions");
            sb.AppendLine();
            AppendResultsTable(sb, regressions);
            sb.AppendLine();
        }

        // Improvements section
        if (improvements.Count > 0)
        {
            sb.AppendLine("### Improvements");
            sb.AppendLine();
            AppendResultsTable(sb, improvements);
            sb.AppendLine();
        }

        // New benchmarks section
        if (newBenchmarks.Count > 0)
        {
            sb.AppendLine("### New Benchmarks");
            sb.AppendLine();
            sb.AppendLine("| Benchmark | Mean (ns) | Alloc (B) |");
            sb.AppendLine("|-----------|-----------|-----------|");
            foreach (var entry in newBenchmarks)
            {
                if (current.TryGetValue(entry.BenchmarkName, out var currentEntry))
                {
                    sb.AppendLine(CultureInfo.InvariantCulture,
                        $"| {ShortenName(entry.BenchmarkName)} " +
                        $"| {FormatNumber(currentEntry.MeanNs)} " +
                        $"| {currentEntry.AllocatedBytes} |");
                }
            }

            sb.AppendLine();
        }

        // All results summary
        sb.AppendLine("### All Results");
        sb.AppendLine();
        sb.AppendLine("| Benchmark | Mean (ns) | Alloc (B) | Status |");
        sb.AppendLine("|-----------|-----------|-----------|--------|");

        foreach (var (name, entry) in current.OrderBy(x => x.Key, StringComparer.Ordinal))
        {
            var status2 = GetBenchmarkStatus(name, results);
            sb.AppendLine(CultureInfo.InvariantCulture,
                $"| {ShortenName(name)} " +
                $"| {FormatNumber(entry.MeanNs)} " +
                $"| {entry.AllocatedBytes} " +
                $"| {status2} |");
        }

        return sb.ToString();
    }

    private static void AppendResultsTable(StringBuilder sb, List<RegressionResult> results)
    {
        sb.AppendLine("| Benchmark | Metric | Baseline | Current | Delta |");
        sb.AppendLine("|-----------|--------|----------|---------|-------|");

        foreach (var result in results.OrderByDescending(r => Math.Abs(r.DeltaPercent)))
        {
            var delta = result.DeltaPercent >= 0
                ? $"+{result.DeltaPercent:F1}%"
                : $"{result.DeltaPercent:F1}%";

            sb.AppendLine(CultureInfo.InvariantCulture,
                $"| {ShortenName(result.BenchmarkName)} " +
                $"| {result.Metric} " +
                $"| {FormatNumber(result.BaselineValue)} " +
                $"| {FormatNumber(result.CurrentValue)} " +
                $"| {delta} |");
        }
    }

    private static string GetBenchmarkStatus(string name, List<RegressionResult> results)
    {
        var benchmarkResults = results.Where(r => r.BenchmarkName == name).ToList();
        if (benchmarkResults.Count == 0)
        {
            return "Stable";
        }

        if (benchmarkResults.Any(r => r.Severity == RegressionSeverity.Regression))
        {
            return "REGRESSION";
        }

        if (benchmarkResults.Any(r => r.Severity == RegressionSeverity.New))
        {
            return "New";
        }

        if (benchmarkResults.Any(r => r.Severity == RegressionSeverity.Improvement))
        {
            return "Improved";
        }

        return "Stable";
    }

    private static string ShortenName(string fullName)
    {
        // Show only the last two segments: Class.Method
        var parts = fullName.Split('.');
        return parts.Length >= 2
            ? $"{parts[^2]}.{parts[^1]}"
            : fullName;
    }

    private static string FormatNumber(double value)
    {
        return value switch
        {
            >= 1_000_000_000 => $"{value / 1_000_000_000:F2}s",
            >= 1_000_000 => $"{value / 1_000_000:F2}ms",
            >= 1_000 => $"{value / 1_000:F2}us",
            _ => $"{value:F2}"
        };
    }
}

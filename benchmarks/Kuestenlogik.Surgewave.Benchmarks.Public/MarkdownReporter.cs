using System.Globalization;
using System.Text;
using Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;

namespace Kuestenlogik.Surgewave.Benchmarks.Public;

/// <summary>
/// Renders a <see cref="PublicBenchmarkReport"/> to a single Markdown
/// document — what gets committed to <c>docs/benchmarks/results-v0.X.md</c>
/// per release. Hardware-snapshot at the top so anyone reading the
/// file later knows whether the numbers apply to their setup, options
/// block right after so the run is reproducible from disk, then one
/// section per scenario with the cross-system table.
/// </summary>
public static class MarkdownReporter
{
    public static string Render(PublicBenchmarkReport report, string? comparedAgainst = null)
    {
        var sb = new StringBuilder();

        sb.AppendLine("# Surgewave Public Benchmark Run");
        sb.AppendLine();
        sb.AppendLine($"_Started: `{report.StartedAt:yyyy-MM-dd HH:mm:ss zzz}` · "
                    + $"Completed: `{report.CompletedAt:yyyy-MM-dd HH:mm:ss zzz}` · "
                    + $"Wall-clock: `{(report.CompletedAt - report.StartedAt).TotalMinutes:F1} min`_");
        sb.AppendLine();

        if (comparedAgainst is not null)
        {
            sb.AppendLine($"> Compared against reference baseline `{comparedAgainst}` — see Δ% columns below.");
            sb.AppendLine();
        }

        sb.AppendLine("## Hardware");
        sb.AppendLine();
        sb.AppendLine(report.Hardware.ToMarkdownTable());
        sb.AppendLine();

        sb.AppendLine("## Run options");
        sb.AppendLine();
        sb.AppendLine("| Field | Value |");
        sb.AppendLine("|---|---|");
        sb.AppendLine($"| Message count | `{report.Options.MessageCount:N0}` |");
        sb.AppendLine($"| Payload size | `{report.Options.PayloadBytes} B` |");
        sb.AppendLine($"| Batch size | `{report.Options.BatchSize:N0}` |");
        sb.AppendLine($"| Compression | `{report.Options.CompressionCodec}` |");
        sb.AppendLine($"| Acks | `{report.Options.Acks}` |");
        sb.AppendLine($"| Replication factor | `{report.Options.ReplicationFactor}` |");
        sb.AppendLine($"| Warmup rounds | `{report.Options.WarmupRounds}` |");
        sb.AppendLine($"| Measurement rounds | `{report.Options.MeasurementRounds}` |");
        sb.AppendLine();

        sb.AppendLine("## Scenarios");
        sb.AppendLine();
        foreach (var scenario in report.Scenarios)
        {
            sb.AppendLine($"### {scenario.Name}");
            sb.AppendLine();
            sb.AppendLine($"_ID: `{scenario.Id}` · Duration: `{scenario.Duration.TotalSeconds:F1} s`_");
            sb.AppendLine();
            sb.AppendLine($"> {scenario.Description}");
            sb.AppendLine();
            sb.Append(RenderScenarioTable(scenario.Rows));
            sb.AppendLine();
        }

        return sb.ToString();
    }

    private static string RenderScenarioTable(IReadOnlyList<ScenarioResult> rows)
    {
        var sb = new StringBuilder();
        sb.AppendLine("| System | msg/s | MB/s | P50 ms | P90 ms | P99 ms | P99.9 ms | P99.99 ms |");
        sb.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var row in rows)
        {
            sb.Append('|').Append(' ').Append(row.System).Append(' ');
            sb.Append('|').Append(' ').Append(Format(row.ThroughputMessagesPerSec, "N0")).Append(' ');
            sb.Append('|').Append(' ').Append(Format(row.ThroughputMegabytesPerSec, "N1")).Append(' ');
            sb.Append('|').Append(' ').Append(FormatNullable(row.P50LatencyMs)).Append(' ');
            sb.Append('|').Append(' ').Append(FormatNullable(row.P90LatencyMs)).Append(' ');
            sb.Append('|').Append(' ').Append(FormatNullable(row.P99LatencyMs)).Append(' ');
            sb.Append('|').Append(' ').Append(FormatNullable(row.P999LatencyMs)).Append(' ');
            sb.Append('|').Append(' ').Append(FormatNullable(row.P9999LatencyMs)).Append(' ');
            sb.AppendLine("|");
        }
        return sb.ToString();
    }

    private static string Format(double value, string fmt) =>
        value.ToString(fmt, CultureInfo.InvariantCulture);

    private static string FormatNullable(double? value) =>
        value is null ? "—" : value.Value.ToString("F3", CultureInfo.InvariantCulture);
}

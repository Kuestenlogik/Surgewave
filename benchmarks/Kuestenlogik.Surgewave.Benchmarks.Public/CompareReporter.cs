using System.Globalization;
using System.Text;
using Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;

namespace Kuestenlogik.Surgewave.Benchmarks.Public;

/// <summary>
/// Renders a Δ% Markdown table comparing two reports — typically the
/// user's local run against a committed reference baseline. Rows
/// match by <c>(scenarioId, system)</c>; rows that exist in one report
/// but not the other are dropped with a note at the top of the
/// section (could be added if it ever becomes useful).
/// </summary>
public static class CompareReporter
{
    public static string Render(PublicBenchmarkReport current, PublicBenchmarkReport reference)
    {
        var sb = new StringBuilder();
        sb.AppendLine("## Comparison vs reference");
        sb.AppendLine();
        sb.AppendLine($"_Reference run: `{reference.StartedAt:yyyy-MM-dd}` on `{reference.Hardware.CpuModel}`_");
        sb.AppendLine();

        foreach (var scenario in current.Scenarios)
        {
            var refScenario = reference.Scenarios.FirstOrDefault(s => s.Id == scenario.Id);
            if (refScenario is null) continue;

            sb.AppendLine($"### {scenario.Name}");
            sb.AppendLine();
            sb.AppendLine("| System | msg/s | Δ% | P99 ms | Δ% | P99.99 ms | Δ% |");
            sb.AppendLine("|---|---:|---:|---:|---:|---:|---:|");

            foreach (var row in scenario.Rows)
            {
                var refRow = refScenario.Rows.FirstOrDefault(r => r.System == row.System);
                if (refRow is null) continue;

                sb.Append('|').Append(' ').Append(row.System).Append(' ');
                sb.Append('|').Append(' ').Append(F(row.ThroughputMessagesPerSec, "N0")).Append(' ');
                sb.Append('|').Append(' ').Append(Delta(row.ThroughputMessagesPerSec, refRow.ThroughputMessagesPerSec, higherIsBetter: true)).Append(' ');
                sb.Append('|').Append(' ').Append(FN(row.P99LatencyMs)).Append(' ');
                sb.Append('|').Append(' ').Append(DeltaN(row.P99LatencyMs, refRow.P99LatencyMs, higherIsBetter: false)).Append(' ');
                sb.Append('|').Append(' ').Append(FN(row.P9999LatencyMs)).Append(' ');
                sb.Append('|').Append(' ').Append(DeltaN(row.P9999LatencyMs, refRow.P9999LatencyMs, higherIsBetter: false)).Append(' ');
                sb.AppendLine("|");
            }
            sb.AppendLine();
        }

        return sb.ToString();
    }

    private static string F(double v, string fmt) => v.ToString(fmt, CultureInfo.InvariantCulture);
    private static string FN(double? v) => v is null ? "—" : v.Value.ToString("F3", CultureInfo.InvariantCulture);

    private static string Delta(double current, double reference, bool higherIsBetter)
    {
        if (reference == 0) return "—";
        var pct = (current - reference) / reference * 100.0;
        var sign = pct >= 0 ? "+" : "";
        // For metrics where higher is worse (latency), invert the sign for the
        // emoji/marker so a regression always reads as "worse" without the
        // reader having to keep the direction in their head.
        var marker = (higherIsBetter ? pct >= 0 : pct <= 0) ? "✓" : "⚠";
        return $"{sign}{pct.ToString("F1", CultureInfo.InvariantCulture)}% {marker}";
    }

    private static string DeltaN(double? current, double? reference, bool higherIsBetter)
    {
        if (current is null || reference is null) return "—";
        return Delta(current.Value, reference.Value, higherIsBetter);
    }
}

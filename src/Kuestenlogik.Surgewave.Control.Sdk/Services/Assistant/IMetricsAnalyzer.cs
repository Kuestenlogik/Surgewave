using Kuestenlogik.Surgewave.Control.Models.Assistant;

namespace Kuestenlogik.Surgewave.Control.Services.Assistant;

/// <summary>
/// Analyzes metrics snapshots to detect anomalies using rule-based heuristics.
/// </summary>
public interface IMetricsAnalyzer
{
    /// <summary>
    /// Records a new metrics snapshot and analyzes it for anomalies.
    /// </summary>
    /// <param name="snapshot">The current metrics snapshot.</param>
    /// <param name="sensitivity">Detection sensitivity from 0.0 (lenient) to 1.0 (strict).</param>
    /// <returns>Any anomalies detected in the current snapshot.</returns>
    Task<List<AnomalyDetection>> AnalyzeAsync(MetricsSnapshot snapshot, double sensitivity = 0.5);
}

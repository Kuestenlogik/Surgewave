using Kuestenlogik.Surgewave.Control.Models.Assistant;

namespace Kuestenlogik.Surgewave.Control.Services.Assistant;

/// <summary>
/// Produces configuration tuning recommendations based on current metrics and detected anomalies.
/// </summary>
public interface ITuningAdvisor
{
    /// <summary>
    /// Evaluates the current metrics snapshot and anomalies to produce actionable tuning recommendations.
    /// </summary>
    List<TuningRecommendation> GetRecommendations(MetricsSnapshot snapshot, List<AnomalyDetection> anomalies);
}

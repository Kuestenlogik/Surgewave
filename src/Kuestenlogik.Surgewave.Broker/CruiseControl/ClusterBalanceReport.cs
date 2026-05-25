using Kuestenlogik.Surgewave.Clustering.Reassignment;

namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// A complete cluster balance analysis report containing per-broker load snapshots,
/// balance scores, detected imbalances, and an optional suggested rebalance plan.
/// </summary>
public sealed class ClusterBalanceReport
{
    /// <summary>
    /// When this report was generated.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Per-broker load snapshots at the time of analysis.
    /// </summary>
    public List<BrokerLoadSnapshot> BrokerLoads { get; init; } = [];

    /// <summary>
    /// Calculated balance scores across all metrics.
    /// </summary>
    public BalanceScore Score { get; init; } = new();

    /// <summary>
    /// Whether the cluster is considered balanced (all metrics within goals).
    /// </summary>
    public bool IsBalanced { get; init; }

    /// <summary>
    /// Details of any detected imbalances that exceed the configured goals.
    /// </summary>
    public List<ImbalanceDetail> Imbalances { get; init; } = [];

    /// <summary>
    /// Suggested reassignment plan to fix the detected imbalances, if any.
    /// </summary>
    public OnlineReassignmentPlan? SuggestedPlan { get; init; }
}

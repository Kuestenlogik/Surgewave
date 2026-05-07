namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// Calculates cluster balance scores and detects imbalances based on broker load snapshots.
/// Uses coefficient of variation (stddev/mean) as the imbalance metric for each dimension.
/// </summary>
public sealed class BalanceCalculator
{
    // Weighted average coefficients for the overall score
    private const double PartitionWeight = 0.30;
    private const double LeaderWeight = 0.25;
    private const double DiskWeight = 0.25;
    private const double NetworkWeight = 0.20;

    /// <summary>
    /// Calculate balance scores for all metrics given a set of broker load snapshots.
    /// Each individual score is 0-100, where 100 = perfectly balanced.
    /// </summary>
    /// <param name="loads">Per-broker load snapshots.</param>
    /// <returns>A <see cref="BalanceScore"/> with per-metric and overall scores.</returns>
    public BalanceScore Calculate(IReadOnlyList<BrokerLoadSnapshot> loads)
    {
        ArgumentNullException.ThrowIfNull(loads);

        if (loads.Count <= 1)
        {
            return new BalanceScore
            {
                PartitionBalance = 100,
                LeaderBalance = 100,
                DiskBalance = 100,
                NetworkBalance = 100,
                OverallScore = 100
            };
        }

        var partitionBalance = CalculateMetricBalance(loads.Select(l => (double)l.PartitionCount).ToArray());
        var leaderBalance = CalculateMetricBalance(loads.Select(l => (double)l.LeaderCount).ToArray());
        var diskBalance = CalculateMetricBalance(loads.Select(l => (double)l.DiskUsageBytes).ToArray());
        var networkBalance = CalculateMetricBalance(
            loads.Select(l => l.ProduceRateBytesPerSec + l.ConsumeRateBytesPerSec).ToArray());

        var overall = (partitionBalance * PartitionWeight)
                    + (leaderBalance * LeaderWeight)
                    + (diskBalance * DiskWeight)
                    + (networkBalance * NetworkWeight);

        return new BalanceScore
        {
            PartitionBalance = partitionBalance,
            LeaderBalance = leaderBalance,
            DiskBalance = diskBalance,
            NetworkBalance = networkBalance,
            OverallScore = Math.Round(overall, 2)
        };
    }

    /// <summary>
    /// Detect specific imbalances that exceed the configured balance goals.
    /// Returns details about the most-loaded and least-loaded brokers for each metric.
    /// </summary>
    /// <param name="loads">Per-broker load snapshots.</param>
    /// <param name="goals">The balance goals that define imbalance thresholds.</param>
    /// <returns>A list of imbalance details for metrics that exceed their thresholds.</returns>
    public List<ImbalanceDetail> DetectImbalances(IReadOnlyList<BrokerLoadSnapshot> loads, BalanceGoals goals)
    {
        ArgumentNullException.ThrowIfNull(loads);
        ArgumentNullException.ThrowIfNull(goals);

        var imbalances = new List<ImbalanceDetail>();

        if (loads.Count <= 1)
            return imbalances;

        CheckMetricImbalance(imbalances, loads, ImbalanceMetric.Partitions,
            l => l.PartitionCount, goals.MaxPartitionImbalancePercent,
            (high, low, pct) => $"Broker {high} has significantly more partitions than broker {low} ({pct:F1}% imbalance)");

        CheckMetricImbalance(imbalances, loads, ImbalanceMetric.Leaders,
            l => l.LeaderCount, goals.MaxLeaderImbalancePercent,
            (high, low, pct) => $"Broker {high} leads significantly more partitions than broker {low} ({pct:F1}% imbalance)");

        CheckMetricImbalance(imbalances, loads, ImbalanceMetric.Disk,
            l => l.DiskUsageBytes, goals.MaxDiskImbalancePercent,
            (high, low, pct) => $"Broker {high} has significantly more disk usage than broker {low} ({pct:F1}% imbalance)");

        CheckMetricImbalance(imbalances, loads, ImbalanceMetric.Network,
            l => l.ProduceRateBytesPerSec + l.ConsumeRateBytesPerSec, goals.MaxNetworkImbalancePercent,
            (high, low, pct) => $"Broker {high} has significantly more network traffic than broker {low} ({pct:F1}% imbalance)");

        return imbalances;
    }

    /// <summary>
    /// Calculates a 0-100 balance score from an array of metric values.
    /// Uses coefficient of variation (stddev / mean * 100) as the imbalance percent.
    /// Score = max(0, 100 - imbalance%).
    /// </summary>
    internal static double CalculateMetricBalance(double[] values)
    {
        if (values.Length <= 1)
            return 100;

        var mean = values.Average();

        // If mean is 0, all values are 0 => perfectly balanced
        if (mean == 0)
            return 100;

        var variance = values.Select(v => (v - mean) * (v - mean)).Average();
        var stddev = Math.Sqrt(variance);
        var imbalancePercent = (stddev / mean) * 100.0;

        return Math.Max(0, Math.Round(100.0 - imbalancePercent, 2));
    }

    private static void CheckMetricImbalance(
        List<ImbalanceDetail> imbalances,
        IReadOnlyList<BrokerLoadSnapshot> loads,
        ImbalanceMetric metric,
        Func<BrokerLoadSnapshot, double> selector,
        double threshold,
        Func<int, int, double, string> descriptionFactory)
    {
        var values = loads.Select(l => (Load: l, Value: selector(l))).ToList();
        var mean = values.Average(v => v.Value);

        if (mean == 0)
            return;

        var max = values.MaxBy(v => v.Value);
        var min = values.MinBy(v => v.Value);

        if (max.Load is null || min.Load is null)
            return;

        // Imbalance % = difference between max and min, as percent of mean
        var imbalancePercent = ((max.Value - min.Value) / mean) * 100.0;

        if (imbalancePercent > threshold)
        {
            imbalances.Add(new ImbalanceDetail
            {
                Metric = metric,
                OverloadedBrokerId = max.Load.BrokerId,
                UnderloadedBrokerId = min.Load.BrokerId,
                ImbalancePercent = Math.Round(imbalancePercent, 2),
                Description = descriptionFactory(max.Load.BrokerId, min.Load.BrokerId, imbalancePercent)
            });
        }
    }
}

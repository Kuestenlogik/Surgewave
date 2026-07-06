using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services.Alerting;

/// <summary>
/// Evaluates alert rules against a broker metrics snapshot, derived per-second
/// rates and consumer-group lag data. Pure logic — fetching the inputs and
/// diffing successive snapshots is the caller's job (<see cref="AlertingService"/>
/// driven by the <see cref="AlertEvaluationWorker"/>).
/// </summary>
public static class AlertRuleEvaluator
{
    public static AlertEvaluationResult Evaluate(
        AlertRule rule,
        MetricsSnapshot metrics,
        MetricsRates rates,
        IReadOnlyList<ConsumerGroupLag> lags,
        bool brokerReachable)
    {
        if (rule.Type == AlertRuleType.BrokerDown)
        {
            return brokerReachable
                ? AlertEvaluationResult.NotTriggered
                : new AlertEvaluationResult(true, 0, "Broker health endpoint is unreachable");
        }

        // An unreachable broker yields an empty metrics snapshot; evaluating
        // threshold rules against it would fire false positives (e.g. LowThroughput).
        if (!brokerReachable)
            return AlertEvaluationResult.NotTriggered;

        return rule.Type switch
        {
            AlertRuleType.ConsumerLag => EvaluateConsumerLag(rule, lags),
            AlertRuleType.ErrorRate => EvaluateErrorRate(rule, rates),
            AlertRuleType.LowThroughput => EvaluateLowThroughput(rule, rates),
            AlertRuleType.HighLatency => new AlertEvaluationResult(
                metrics.ProduceLatencyP99 > rule.Threshold,
                metrics.ProduceLatencyP99,
                $"P99 produce latency {metrics.ProduceLatencyP99:N1}ms exceeds threshold {rule.Threshold}ms"),
            AlertRuleType.DiskUsage => new AlertEvaluationResult(
                metrics.TotalLogSizeBytes > rule.Threshold,
                metrics.TotalLogSizeBytes,
                $"Total log size {metrics.TotalLogSizeBytes:N0} bytes exceeds threshold {rule.Threshold:N0} bytes"),
            // UnderReplicatedPartitions needs in-sync-replica data that lives in the
            // clustering layer, not in the Prometheus metrics snapshot — no template
            // offers it (see AlertsDashboard) until a dedicated feed exists.
            _ => AlertEvaluationResult.NotTriggered,
        };
    }

    private static AlertEvaluationResult EvaluateConsumerLag(AlertRule rule, IReadOnlyList<ConsumerGroupLag> lags)
    {
        if (string.IsNullOrWhiteSpace(rule.Target))
        {
            var maxLag = lags.Count > 0 ? lags.Max(g => g.TotalLag) : 0;
            return new AlertEvaluationResult(maxLag > rule.Threshold, maxLag,
                $"Max consumer lag {maxLag:N0} exceeds threshold {rule.Threshold:N0}");
        }

        var targetGroup = lags.FirstOrDefault(g => g.GroupId == rule.Target);
        if (targetGroup == null)
            return AlertEvaluationResult.NotTriggered;

        return new AlertEvaluationResult(targetGroup.TotalLag > rule.Threshold, targetGroup.TotalLag,
            $"Consumer group '{rule.Target}' lag {targetGroup.TotalLag:N0} exceeds threshold {rule.Threshold:N0}");
    }

    private static AlertEvaluationResult EvaluateErrorRate(AlertRule rule, MetricsRates rates)
    {
        // The broker exposes only cumulative error counters; the current error
        // rate is the delta over elapsed time, unavailable until we have two
        // snapshots to diff.
        if (!rates.Available)
            return AlertEvaluationResult.NotTriggered;

        return new AlertEvaluationResult(
            rates.ErrorsPerSecond > rule.Threshold,
            rates.ErrorsPerSecond,
            $"Error rate {rates.ErrorsPerSecond:N1}/s exceeds threshold {rule.Threshold:N0}/s");
    }

    private static AlertEvaluationResult EvaluateLowThroughput(AlertRule rule, MetricsRates rates)
    {
        // Throughput is a rate (messages/second), computed by diffing the
        // cumulative produced-message counter over elapsed time. Without a prior
        // snapshot there is no rate to compare, so the rule stays quiet rather
        // than firing off the absolute lifetime total.
        if (!rates.Available)
            return AlertEvaluationResult.NotTriggered;

        return new AlertEvaluationResult(
            rates.MessagesProducedPerSecond <= rule.Threshold,
            rates.MessagesProducedPerSecond,
            $"Throughput {rates.MessagesProducedPerSecond:N1} msg/s at or below threshold {rule.Threshold:N0} msg/s");
    }
}

using Kuestenlogik.Surgewave.Control.Models;
using Kuestenlogik.Surgewave.Control.Services;
using Kuestenlogik.Surgewave.Control.Services.Alerting;

namespace Kuestenlogik.Surgewave.Control.Tests.Services;

/// <summary>
/// Pure-logic tests for the server-side alert rule evaluation (#38). ErrorRate
/// and LowThroughput are rate-based (per-second deltas), so they take a
/// <see cref="MetricsRates"/> and must stay quiet until rates are available.
/// </summary>
public sealed class AlertRuleEvaluatorTests
{
    private static AlertRule Rule(AlertRuleType type, double threshold, string? target = null) => new()
    {
        Name = "test",
        Type = type,
        Threshold = threshold,
        Target = target,
        Enabled = true,
    };

    private static ConsumerGroupLag Lag(string groupId, long totalLag)
        => new(groupId, "Stable", totalLag, []);

    private static AlertEvaluationResult Evaluate(
        AlertRule rule,
        MetricsSnapshot? metrics = null,
        MetricsRates? rates = null,
        IReadOnlyList<ConsumerGroupLag>? lags = null,
        bool brokerReachable = true)
        => AlertRuleEvaluator.Evaluate(
            rule,
            metrics ?? new MetricsSnapshot(),
            rates ?? MetricsRates.Unavailable,
            lags ?? [],
            brokerReachable);

    [Fact]
    public void ConsumerLag_WithoutTarget_UsesMaxLagAcrossGroups()
    {
        var result = Evaluate(Rule(AlertRuleType.ConsumerLag, 1000), lags: [Lag("a", 500), Lag("b", 5000)]);

        Assert.True(result.Triggered);
        Assert.Equal(5000, result.CurrentValue);
    }

    [Fact]
    public void ConsumerLag_WithTarget_OnlyEvaluatesThatGroup()
    {
        var result = Evaluate(Rule(AlertRuleType.ConsumerLag, 1000, target: "a"), lags: [Lag("a", 500), Lag("b", 5000)]);

        Assert.False(result.Triggered);
    }

    [Fact]
    public void ConsumerLag_WithUnknownTarget_DoesNotTrigger()
    {
        var result = Evaluate(Rule(AlertRuleType.ConsumerLag, 0, target: "missing"), lags: [Lag("a", 99999)]);

        Assert.False(result.Triggered);
    }

    [Fact]
    public void ErrorRate_TriggersOnPerSecondRateAboveThreshold()
    {
        var rates = new MetricsRates(MessagesProducedPerSecond: 0, ErrorsPerSecond: 12, Available: true);

        var result = Evaluate(Rule(AlertRuleType.ErrorRate, 10), rates: rates);

        Assert.True(result.Triggered);
        Assert.Equal(12, result.CurrentValue);
    }

    [Fact]
    public void ErrorRate_DoesNotFireOnStaleCumulativeCount_WhenRateIsUnavailable()
    {
        // Even a broker with millions of lifetime errors must stay quiet until a
        // real per-second rate can be computed from two snapshots.
        var metrics = new MetricsSnapshot { ErrorsTotal = 1_000_000, ProduceErrorsTotal = 1_000_000 };

        var result = Evaluate(Rule(AlertRuleType.ErrorRate, 10), metrics: metrics, rates: MetricsRates.Unavailable);

        Assert.False(result.Triggered);
    }

    [Fact]
    public void HighLatency_TriggersOnP99AboveThreshold()
    {
        var metrics = new MetricsSnapshot { ProduceLatencyP99 = 150.5 };

        var result = Evaluate(Rule(AlertRuleType.HighLatency, 100), metrics: metrics);

        Assert.True(result.Triggered);
        Assert.Equal(150.5, result.CurrentValue);
    }

    [Fact]
    public void LowThroughput_TriggersWhenRateAtOrBelowThreshold()
    {
        var rates = new MetricsRates(MessagesProducedPerSecond: 0, ErrorsPerSecond: 0, Available: true);

        var result = Evaluate(Rule(AlertRuleType.LowThroughput, 0), rates: rates);

        Assert.True(result.Triggered);
    }

    [Fact]
    public void LowThroughput_DoesNotFireOnHealthyRate()
    {
        var rates = new MetricsRates(MessagesProducedPerSecond: 5000, ErrorsPerSecond: 0, Available: true);

        var result = Evaluate(Rule(AlertRuleType.LowThroughput, 100), rates: rates);

        Assert.False(result.Triggered);
    }

    [Fact]
    public void LowThroughput_DoesNotFireOnBusyBroker_WhenRateUnavailable()
    {
        // A broker with a huge lifetime total but no computed rate must not be
        // reported as idle (the old bug fired off the cumulative counter).
        var metrics = new MetricsSnapshot { MessagesProducedTotal = 1_000_000 };

        var result = Evaluate(Rule(AlertRuleType.LowThroughput, 0), metrics: metrics, rates: MetricsRates.Unavailable);

        Assert.False(result.Triggered);
    }

    [Fact]
    public void BrokerDown_TriggersOnlyWhenUnreachable()
    {
        var rule = Rule(AlertRuleType.BrokerDown, 0);

        Assert.True(Evaluate(rule, brokerReachable: false).Triggered);
        Assert.False(Evaluate(rule, brokerReachable: true).Triggered);
    }

    [Fact]
    public void UnreachableBroker_SuppressesThresholdRules()
    {
        var result = Evaluate(Rule(AlertRuleType.HighLatency, 0), metrics: new MetricsSnapshot { ProduceLatencyP99 = 999 }, brokerReachable: false);

        Assert.False(result.Triggered);
    }

    [Fact]
    public void DiskUsage_ComparesTotalLogSizeBytes()
    {
        var metrics = new MetricsSnapshot { TotalLogSizeBytes = 2_000_000 };

        var result = Evaluate(Rule(AlertRuleType.DiskUsage, 1_000_000), metrics: metrics);

        Assert.True(result.Triggered);
        Assert.Equal(2_000_000, result.CurrentValue);
    }

    [Fact]
    public void UnderReplicatedPartitions_NeverTriggers_NoMetricSource()
    {
        // No template offers this and the evaluator has no data source for it; a
        // legacy rule carrying this type must be an explicit no-op, not a crash.
        var result = Evaluate(
            Rule(AlertRuleType.UnderReplicatedPartitions, 1),
            rates: new MetricsRates(0, 0, Available: true));

        Assert.False(result.Triggered);
    }
}

using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// Evaluates cluster metrics and produces scaling decisions (scale up, scale down, or no change).
/// Implements a stabilization window to prevent flapping between scale-up and scale-down events.
/// </summary>
public sealed class ScaleDecisionEngine
{
    private readonly ILogger<ScaleDecisionEngine> _logger;
    private readonly ServerlessConfig _config;

    private DateTimeOffset _lastScaleDecisionTime = DateTimeOffset.MinValue;
    private ScaleAction _lastScaleAction = ScaleAction.NoChange;

    public ScaleDecisionEngine(
        ILogger<ScaleDecisionEngine> logger,
        ServerlessConfig config)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <summary>
    /// Evaluate the given metrics and return a scaling decision.
    /// </summary>
    /// <param name="metrics">Current cluster metrics snapshot.</param>
    /// <returns>A <see cref="ScaleDecision"/> with the recommended action.</returns>
    public ScaleDecision Evaluate(ScaleMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(metrics);

        var currentCount = metrics.CurrentBrokerCount;

        // Check stabilization window: if we recently made a scaling decision,
        // hold off to prevent flapping.
        if (_lastScaleAction != ScaleAction.NoChange &&
            metrics.Timestamp - _lastScaleDecisionTime < _config.StabilizationWindow)
        {
            _logger.LogDebug(
                "Within stabilization window ({Remaining}ms remaining), returning NoChange",
                (_config.StabilizationWindow - (metrics.Timestamp - _lastScaleDecisionTime)).TotalMilliseconds);

            return new ScaleDecision(ScaleAction.NoChange, "Within stabilization window", currentCount);
        }

        // Determine if we should scale up
        if (ShouldScaleUp(metrics, currentCount, out var scaleUpReason))
        {
            var targetCount = Math.Min(currentCount + 1, _config.MaxBrokers);

            if (targetCount == currentCount)
            {
                return new ScaleDecision(ScaleAction.NoChange,
                    $"Would scale up but already at max brokers ({_config.MaxBrokers})",
                    currentCount);
            }

            RecordDecision(ScaleAction.ScaleUp, metrics.Timestamp);

            _logger.LogInformation(
                "Scale UP decision: {Reason}. Target brokers: {Current} -> {Target}",
                scaleUpReason, currentCount, targetCount);

            return new ScaleDecision(ScaleAction.ScaleUp, scaleUpReason, targetCount);
        }

        // Determine if we should scale down
        if (ShouldScaleDown(metrics, currentCount, out var scaleDownReason))
        {
            var targetCount = Math.Max(currentCount - 1, _config.MinBrokers);

            if (targetCount == currentCount)
            {
                return new ScaleDecision(ScaleAction.NoChange,
                    $"Would scale down but already at min brokers ({_config.MinBrokers})",
                    currentCount);
            }

            RecordDecision(ScaleAction.ScaleDown, metrics.Timestamp);

            _logger.LogInformation(
                "Scale DOWN decision: {Reason}. Target brokers: {Current} -> {Target}",
                scaleDownReason, currentCount, targetCount);

            return new ScaleDecision(ScaleAction.ScaleDown, scaleDownReason, targetCount);
        }

        return new ScaleDecision(ScaleAction.NoChange, "Cluster is appropriately sized", currentCount);
    }

    /// <summary>
    /// Allows resetting the stabilization window for testing purposes.
    /// </summary>
    internal void ResetStabilizationWindow()
    {
        _lastScaleDecisionTime = DateTimeOffset.MinValue;
        _lastScaleAction = ScaleAction.NoChange;
    }

    private bool ShouldScaleUp(ScaleMetrics metrics, int currentCount, out string reason)
    {
        // CPU above threshold
        if (metrics.CpuUsagePercent >= _config.ScaleUpThresholdPercent)
        {
            reason = $"CPU usage {metrics.CpuUsagePercent:F1}% exceeds threshold {_config.ScaleUpThresholdPercent}%";
            return true;
        }

        // High combined throughput with active connections indicates load
        var totalRate = metrics.ProduceRatePerSecond + metrics.FetchRatePerSecond;
        if (totalRate > 0 && metrics.ActiveConnections > currentCount * 50)
        {
            reason = $"High connection density: {metrics.ActiveConnections} connections across {currentCount} brokers";
            return true;
        }

        reason = string.Empty;
        return false;
    }

    private bool ShouldScaleDown(ScaleMetrics metrics, int currentCount, out string reason)
    {
        // True scale-to-zero: no connections, no throughput, no unflushed data
        if (metrics.ActiveConnections == 0 &&
            metrics.ProduceRatePerSecond == 0 &&
            metrics.FetchRatePerSecond == 0 &&
            metrics.UnflushedBytes == 0 &&
            _config.MinBrokers == 0)
        {
            reason = "Cluster is completely idle, scaling to zero";
            return true;
        }

        // CPU below threshold
        if (metrics.CpuUsagePercent <= _config.ScaleDownThresholdPercent &&
            currentCount > _config.MinBrokers)
        {
            reason = $"CPU usage {metrics.CpuUsagePercent:F1}% below threshold {_config.ScaleDownThresholdPercent}%";
            return true;
        }

        reason = string.Empty;
        return false;
    }

    private void RecordDecision(ScaleAction action, DateTimeOffset timestamp)
    {
        _lastScaleAction = action;
        _lastScaleDecisionTime = timestamp;
    }
}

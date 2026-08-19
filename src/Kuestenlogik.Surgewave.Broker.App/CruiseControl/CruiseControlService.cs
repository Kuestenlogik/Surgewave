using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Reassignment;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// Background service that continuously monitors cluster balance and generates
/// or executes rebalance plans. Similar to Confluent's Cruise Control.
/// </summary>
public sealed partial class CruiseControlService : BackgroundService
{
    private readonly CruiseControlConfig _config;
    private readonly LoadCollector _loadCollector;
    private readonly BalanceCalculator _balanceCalculator;
    private readonly ReassignmentPlanner _reassignmentPlanner;
    private readonly ReassignmentExecutor _reassignmentExecutor;
    private readonly ILogger<CruiseControlService> _logger;

    private readonly ConcurrentQueue<ClusterBalanceReport> _history = new();
    private const int MaxHistorySize = 100;

    private volatile ClusterBalanceReport? _latestReport;
    private DateTimeOffset? _lastRebalanceTime;

    /// <summary>
    /// Initializes a new instance of <see cref="CruiseControlService"/>.
    /// </summary>
    public CruiseControlService(
        CruiseControlConfig config,
        LoadCollector loadCollector,
        BalanceCalculator balanceCalculator,
        ReassignmentPlanner reassignmentPlanner,
        ReassignmentExecutor reassignmentExecutor,
        ILogger<CruiseControlService> logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _loadCollector = loadCollector ?? throw new ArgumentNullException(nameof(loadCollector));
        _balanceCalculator = balanceCalculator ?? throw new ArgumentNullException(nameof(balanceCalculator));
        _reassignmentPlanner = reassignmentPlanner ?? throw new ArgumentNullException(nameof(reassignmentPlanner));
        _reassignmentExecutor = reassignmentExecutor ?? throw new ArgumentNullException(nameof(reassignmentExecutor));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Gets the current Cruise Control configuration.
    /// </summary>
    public CruiseControlConfig Config => _config;

    /// <summary>
    /// Gets the most recent cluster balance report, or null if no analysis has been performed yet.
    /// </summary>
    public ClusterBalanceReport? GetLatestReport() => _latestReport;

    /// <summary>
    /// Gets the most recent balance reports, up to the specified count.
    /// </summary>
    /// <param name="count">Maximum number of reports to return.</param>
    /// <returns>A list of recent balance reports, newest first.</returns>
    public IReadOnlyList<ClusterBalanceReport> GetHistory(int count = 10)
    {
        return _history.Reverse().Take(count).ToList();
    }

    /// <summary>
    /// Force an immediate balance analysis outside the normal interval.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The generated cluster balance report.</returns>
    public async Task<ClusterBalanceReport> AnalyzeNowAsync(CancellationToken ct = default)
    {
        return await PerformAnalysisAsync(ct);
    }

    /// <summary>
    /// Apply the most recent suggested rebalance plan, if one exists.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the plan has been submitted for execution.</returns>
    public async Task ApplySuggestionAsync(CancellationToken ct = default)
    {
        var report = _latestReport;
        if (report?.SuggestedPlan is null)
        {
            LogNoSuggestion();
            return;
        }

        if (report.SuggestedPlan.Assignments.Count == 0)
        {
            LogEmptyPlan();
            return;
        }

        LogApplyingSuggestion(report.SuggestedPlan.Assignments.Count);
        report.SuggestedPlan.ThrottleRateBytesPerSec = _config.ThrottleRateBytesPerSec;
        await _reassignmentExecutor.ExecuteAsync(report.SuggestedPlan, ct);
        _lastRebalanceTime = DateTimeOffset.UtcNow;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            LogDisabled();
            return;
        }

        LogStarted(_config.Mode.ToString(), _config.AnalysisIntervalSeconds);

        var interval = TimeSpan.FromSeconds(_config.AnalysisIntervalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, stoppingToken);
                await PerformAnalysisAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                LogAnalysisError(ex);
            }
        }

        LogStopped();
    }

    internal async Task<ClusterBalanceReport> PerformAnalysisAsync(CancellationToken ct)
    {
        var loads = await _loadCollector.CollectAsync(ct);

        var score = _balanceCalculator.Calculate(loads);
        var imbalances = _balanceCalculator.DetectImbalances(loads, _config.Goals);
        var isBalanced = imbalances.Count == 0;

        OnlineReassignmentPlan? suggestedPlan = null;

        if (!isBalanced)
        {
            // Get current assignments and broker list for the planner
            var currentAssignments = _reassignmentExecutor.GetCurrentAssignments();
            var brokerIds = loads.Select(l => l.BrokerId).ToList();

            if (currentAssignments.Count > 0 && brokerIds.Count > 1)
            {
                suggestedPlan = _reassignmentPlanner.GenerateBalancePlan(currentAssignments, brokerIds);

                // Filter out plans that are too small to be worth executing
                if (suggestedPlan.Assignments.Count < _config.Goals.MinPartitionsToRebalance)
                {
                    suggestedPlan = null;
                }
            }
        }

        var report = new ClusterBalanceReport
        {
            Timestamp = DateTimeOffset.UtcNow,
            BrokerLoads = loads.ToList(),
            Score = score,
            IsBalanced = isBalanced,
            Imbalances = imbalances,
            SuggestedPlan = suggestedPlan
        };

        _latestReport = report;
        AddToHistory(report);

        LogAnalysisComplete(score.OverallScore, isBalanced, imbalances.Count,
            suggestedPlan?.Assignments.Count ?? 0);

        // Auto-rebalance if mode allows and cooldown has passed
        if (_config.Mode == CruiseControlMode.AutoRebalance && suggestedPlan is not null)
        {
            if (IsCooldownExpired())
            {
                LogAutoRebalanceTriggered(suggestedPlan.Assignments.Count);
                suggestedPlan.ThrottleRateBytesPerSec = _config.ThrottleRateBytesPerSec;
                await _reassignmentExecutor.ExecuteAsync(suggestedPlan, ct);
                _lastRebalanceTime = DateTimeOffset.UtcNow;
            }
            else
            {
                LogCooldownActive(_config.CooldownMinutes);
            }
        }

        return report;
    }

    private bool IsCooldownExpired()
    {
        if (_lastRebalanceTime is null)
            return true;

        var elapsed = DateTimeOffset.UtcNow - _lastRebalanceTime.Value;
        return elapsed.TotalMinutes >= _config.CooldownMinutes;
    }

    private void AddToHistory(ClusterBalanceReport report)
    {
        _history.Enqueue(report);

        while (_history.Count > MaxHistorySize)
        {
            _history.TryDequeue(out _);
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Cruise Control is disabled")]
    private partial void LogDisabled();

    [LoggerMessage(Level = LogLevel.Information, Message = "Cruise Control started (mode={Mode}, interval={Interval}s)")]
    private partial void LogStarted(string mode, int interval);

    [LoggerMessage(Level = LogLevel.Information, Message = "Cruise Control stopped")]
    private partial void LogStopped();

    [LoggerMessage(Level = LogLevel.Information, Message = "Cruise Control analysis complete: score={Score:F1}, balanced={IsBalanced}, imbalances={ImbalanceCount}, suggested moves={SuggestedMoves}")]
    private partial void LogAnalysisComplete(double score, bool isBalanced, int imbalanceCount, int suggestedMoves);

    [LoggerMessage(Level = LogLevel.Information, Message = "Auto-rebalance triggered: executing plan with {MoveCount} partition move(s)")]
    private partial void LogAutoRebalanceTriggered(int moveCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Auto-rebalance skipped: cooldown active ({CooldownMinutes} min between rebalances)")]
    private partial void LogCooldownActive(int cooldownMinutes);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No suggested rebalance plan to apply")]
    private partial void LogNoSuggestion();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Suggested plan has no assignments")]
    private partial void LogEmptyPlan();

    [LoggerMessage(Level = LogLevel.Information, Message = "Applying suggested rebalance plan with {MoveCount} partition move(s)")]
    private partial void LogApplyingSuggestion(int moveCount);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error during Cruise Control analysis cycle")]
    private partial void LogAnalysisError(Exception ex);
}

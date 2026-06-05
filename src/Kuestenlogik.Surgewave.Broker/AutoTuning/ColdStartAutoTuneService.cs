using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.AutoTuning;

/// <summary>
/// Background service that owns the broker's <see cref="ColdStartWorkloadProfiler"/>,
/// waits for the configured observation window to elapse, then runs
/// <see cref="ColdStartTuningRecommender"/> against the resulting profile,
/// persists the result to <see cref="ColdStartAutoTuneConfig.AutoTunedJsonPath"/>,
/// and (optionally) applies the recommendations live via
/// <see cref="DynamicBrokerConfig"/>.
/// </summary>
public sealed class ColdStartAutoTuneService : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly ColdStartAutoTuneConfig _config;
    private readonly BrokerConfig _brokerConfig;
    private readonly DynamicBrokerConfig _dynamicConfig;
    private readonly ColdStartWorkloadProfiler _profiler;
    private readonly TimeProvider _time;
    private readonly ILogger<ColdStartAutoTuneService> _logger;

    private int _hasReported;

    public ColdStartAutoTuneService(
        ColdStartAutoTuneConfig config,
        BrokerConfig brokerConfig,
        DynamicBrokerConfig dynamicConfig,
        ColdStartWorkloadProfiler profiler,
        ILogger<ColdStartAutoTuneService> logger,
        TimeProvider? timeProvider = null)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _brokerConfig = brokerConfig ?? throw new ArgumentNullException(nameof(brokerConfig));
        _dynamicConfig = dynamicConfig ?? throw new ArgumentNullException(nameof(dynamicConfig));
        _profiler = profiler ?? throw new ArgumentNullException(nameof(profiler));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Whether the service has already emitted its one-shot
    /// recommendation cycle. Visible for tests.</summary>
    public bool HasReported => Volatile.Read(ref _hasReported) == 1;

    /// <inheritdoc/>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogDebug("Cold-start auto-tune is disabled");
            return;
        }

        _logger.LogInformation(
            "Cold-start auto-tune started (window={Window}, autoApply={AutoApply}, output={Path})",
            _config.ObservationWindow, _config.AutoApply, LogSanitizer.Sanitize(_config.AutoTunedJsonPath));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_config.CheckInterval, _time, stoppingToken).ConfigureAwait(false);
                if (TryReportOnce())
                {
                    // One-shot service — we're done.
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during cold-start auto-tune cycle");
            }
        }

        _logger.LogInformation("Cold-start auto-tune stopped");
    }

    /// <summary>
    /// Visible for tests: if the observation window has elapsed and we
    /// have not reported yet, build the profile, run the recommender,
    /// persist and (optionally) apply. Returns <c>true</c> if reporting
    /// happened on this call.
    /// </summary>
    internal bool TryReportOnce()
    {
        if (Volatile.Read(ref _hasReported) == 1) return false;
        if (!_profiler.IsComplete) return false;
        if (Interlocked.CompareExchange(ref _hasReported, 1, 0) != 0) return false;

        var profile = _profiler.BuildProfile();
        var snapshot = SnapshotBrokerConfig();
        var recommendations = ColdStartTuningRecommender.Recommend(profile, snapshot);

        var applied = new List<string>();
        if (_config.AutoApply)
        {
            foreach (var r in recommendations)
            {
                var error = _dynamicConfig.SetConfig(r.ConfigKey, r.SuggestedValue);
                if (error is null)
                {
                    applied.Add(r.RuleId);
                    _logger.LogInformation(
                        "Cold-start auto-apply: {RuleId} {ConfigKey} {Old} -> {New}",
                        LogSanitizer.Sanitize(r.RuleId),
                        LogSanitizer.Sanitize(r.ConfigKey),
                        LogSanitizer.Sanitize(r.CurrentValue),
                        LogSanitizer.Sanitize(r.SuggestedValue));
                }
                else
                {
                    _logger.LogWarning(
                        "Cold-start auto-apply failed: {RuleId} ({ConfigKey}): {Error}",
                        LogSanitizer.Sanitize(r.RuleId),
                        LogSanitizer.Sanitize(r.ConfigKey),
                        LogSanitizer.Sanitize(error));
                }
            }
        }

        WriteReport(profile, snapshot, recommendations, applied);

        _logger.LogInformation(
            "Cold-start auto-tune complete: {Count} recommendation(s), {Applied} applied, report -> {Path}",
            recommendations.Count,
            applied.Count,
            LogSanitizer.Sanitize(_config.AutoTunedJsonPath));

        return true;
    }

    private ColdStartBrokerSnapshot SnapshotBrokerConfig() => new(
        DefaultNumPartitions: _brokerConfig.DefaultNumPartitions,
        ProducerBatchSizeBytes: _brokerConfig.ProducerBatchSizeBytes,
        ReplicaFetchMaxBytes: _brokerConfig.ReplicaFetchMaxBytes,
        LogSegmentBytes: _brokerConfig.LogSegmentBytes);

    private void WriteReport(
        WorkloadProfile profile,
        ColdStartBrokerSnapshot snapshot,
        IReadOnlyList<AutoTuningRecommendation> recommendations,
        IReadOnlyList<string> applied)
    {
        var report = new ColdStartAutoTuneReport(
            GeneratedAt: _time.GetUtcNow(),
            ObservationWindow: _config.ObservationWindow,
            AutoApplied: _config.AutoApply,
            Profile: profile,
            Snapshot: snapshot,
            Recommendations: recommendations,
            AppliedRuleIds: applied);

        try
        {
            var json = JsonSerializer.Serialize(report, JsonOptions);
            var dir = Path.GetDirectoryName(_config.AutoTunedJsonPath);
            if (!string.IsNullOrEmpty(dir))
            {
                Directory.CreateDirectory(dir);
            }
            File.WriteAllText(_config.AutoTunedJsonPath, json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Cold-start auto-tune: failed to write report to {Path}",
                LogSanitizer.Sanitize(_config.AutoTunedJsonPath));
        }
    }
}

/// <summary>
/// JSON-serialised audit record written to
/// <see cref="ColdStartAutoTuneConfig.AutoTunedJsonPath"/>. Operators read
/// this to understand which recommendations the broker derived from the
/// cold-start observation window and which were auto-applied.
/// </summary>
public sealed record ColdStartAutoTuneReport(
    DateTimeOffset GeneratedAt,
    TimeSpan ObservationWindow,
    bool AutoApplied,
    WorkloadProfile Profile,
    ColdStartBrokerSnapshot Snapshot,
    IReadOnlyList<AutoTuningRecommendation> Recommendations,
    IReadOnlyList<string> AppliedRuleIds);

using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.AutoTuning;

/// <summary>
/// Background service that monitors broker metrics and generates or applies
/// configuration tuning recommendations based on workload patterns.
/// </summary>
public sealed class AutoTuningService : BackgroundService
{
    private readonly AutoTuningConfig _config;
    private readonly BrokerConfig _brokerConfig;
    private readonly DynamicBrokerConfig _dynamicConfig;
    private readonly BrokerMetrics _metrics;
    private readonly ILogger<AutoTuningService> _logger;

    private readonly ConcurrentDictionary<string, AutoTuningRecommendation> _activeRecommendations = new();
    private readonly ConcurrentQueue<AutoTuningRecommendation> _history = new();
    private const int MaxHistorySize = 500;

    /// <summary>
    /// Initializes a new instance of <see cref="AutoTuningService"/>.
    /// </summary>
    public AutoTuningService(
        AutoTuningConfig config,
        BrokerConfig brokerConfig,
        DynamicBrokerConfig dynamicConfig,
        BrokerMetrics metrics,
        ILogger<AutoTuningService> logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _brokerConfig = brokerConfig ?? throw new ArgumentNullException(nameof(brokerConfig));
        _dynamicConfig = dynamicConfig ?? throw new ArgumentNullException(nameof(dynamicConfig));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Gets all currently active (pending) recommendations.
    /// </summary>
    public IReadOnlyList<AutoTuningRecommendation> ActiveRecommendations =>
        _activeRecommendations.Values.ToList();

    /// <summary>
    /// Gets the history of all generated recommendations (including applied ones).
    /// </summary>
    public IReadOnlyList<AutoTuningRecommendation> History =>
        _history.ToList();

    /// <summary>
    /// Gets the current auto-tuning configuration.
    /// </summary>
    public AutoTuningConfig Config => _config;

    /// <summary>
    /// Manually applies a specific recommendation by rule ID.
    /// </summary>
    /// <param name="ruleId">The rule ID of the recommendation to apply.</param>
    /// <returns>The applied recommendation, or null if the rule ID was not found.</returns>
    public AutoTuningRecommendation? ApplyRecommendation(string ruleId)
    {
        if (!_activeRecommendations.TryRemove(ruleId, out var recommendation))
            return null;

        var error = _dynamicConfig.SetConfig(recommendation.ConfigKey, recommendation.SuggestedValue);
        if (error is not null)
        {
            _logger.LogWarning(
                "Failed to apply auto-tuning recommendation {RuleId}: {Error}",
                LogSanitizer.Sanitize(ruleId), LogSanitizer.Sanitize(error));
            // Put it back if it failed
            _activeRecommendations.TryAdd(ruleId, recommendation);
            return null;
        }

        var applied = recommendation with
        {
            WasAutoApplied = false, // manually applied
            Timestamp = DateTimeOffset.UtcNow
        };

        AddToHistory(applied);
        _logger.LogInformation(
            "Auto-tuning recommendation applied: {RuleId} - {ConfigKey} changed from {Old} to {New}",
            LogSanitizer.Sanitize(ruleId), LogSanitizer.Sanitize(recommendation.ConfigKey), recommendation.CurrentValue, recommendation.SuggestedValue);

        return applied;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogDebug("Auto-tuning is disabled");
            return;
        }

        _logger.LogInformation(
            "Auto-tuning started (mode={Mode}, interval={Interval}s)",
            _config.Mode, _config.AnalysisIntervalSeconds);

        var interval = TimeSpan.FromSeconds(_config.AnalysisIntervalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, stoppingToken);
                AnalyzeAndRecommend();
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during auto-tuning analysis cycle");
            }
        }

        _logger.LogInformation("Auto-tuning stopped");
    }

    internal void AnalyzeAndRecommend()
    {
        EvaluateBatchSizeRule();
        EvaluateCompressionRule();
        EvaluateFetchSizeRule();
        EvaluateThreadPoolRule();
        EvaluateBufferMemoryRule();
        EvaluatePartitionHotspotRule();
        EvaluateIsrRule();
        EvaluateLogSegmentRule();
    }

    private void EvaluateBatchSizeRule()
    {
        const string ruleId = "batch-size";
        if (IsRuleDisabled(ruleId)) return;

        // Rule: if producer batch size < 64KB, suggest increase
        var currentBatchSize = _brokerConfig.ProducerBatchSizeBytes;
        if (currentBatchSize < 65536)
        {
            var suggested = Math.Min(currentBatchSize * 2, 65536);
            EmitRecommendation(new AutoTuningRecommendation
            {
                RuleId = ruleId,
                Description = "Increase producer batch size for better throughput",
                ConfigKey = "producer.batch.size",
                CurrentValue = currentBatchSize.ToString(),
                SuggestedValue = suggested.ToString(),
                Reason = $"Current batch size ({currentBatchSize} bytes) is below optimal threshold of 64KB. Larger batches improve throughput by amortizing network overhead.",
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }

    private void EvaluateCompressionRule()
    {
        const string ruleId = "compression";
        if (IsRuleDisabled(ruleId)) return;

        // Rule: if compression is not enabled, suggest it
        var currentCompression = _dynamicConfig.GetConfig("compression.type");
        if (string.IsNullOrEmpty(currentCompression) ||
            currentCompression.Equals("none", StringComparison.OrdinalIgnoreCase))
        {
            EmitRecommendation(new AutoTuningRecommendation
            {
                RuleId = ruleId,
                Description = "Enable compression to reduce network and storage overhead",
                ConfigKey = "compression.type",
                CurrentValue = currentCompression ?? "none",
                SuggestedValue = "lz4",
                Reason = "Compression is not enabled. LZ4 provides fast compression with minimal CPU overhead, typically reducing message sizes by 50-70%.",
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }

    private void EvaluateFetchSizeRule()
    {
        const string ruleId = "fetch-size";
        if (IsRuleDisabled(ruleId)) return;

        // Rule: if fetch max bytes < 1MB, suggest increase
        var currentFetchMax = _brokerConfig.ReplicaFetchMaxBytes;
        if (currentFetchMax < 1_048_576)
        {
            EmitRecommendation(new AutoTuningRecommendation
            {
                RuleId = ruleId,
                Description = "Increase fetch max bytes for consumers with growing lag",
                ConfigKey = "replica.fetch.max.bytes",
                CurrentValue = currentFetchMax.ToString(),
                SuggestedValue = "1048576",
                Reason = $"Current fetch max bytes ({currentFetchMax}) is below 1MB. Increasing it allows consumers to fetch larger batches, reducing the number of fetch requests and improving throughput.",
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }

    private void EvaluateThreadPoolRule()
    {
        const string ruleId = "thread-pool";
        if (IsRuleDisabled(ruleId)) return;

        // Rule: if channel write workers are too low relative to processor count
        var processorCount = Environment.ProcessorCount;
        var currentWorkers = _brokerConfig.ChannelWriteWorkers;
        var effectiveWorkers = currentWorkers == 0
            ? Math.Max(processorCount * 2, 8)
            : currentWorkers;

        if (effectiveWorkers < processorCount)
        {
            EmitRecommendation(new AutoTuningRecommendation
            {
                RuleId = ruleId,
                Description = "Increase channel write workers to match processor count",
                ConfigKey = "channel.write.workers",
                CurrentValue = effectiveWorkers.ToString(),
                SuggestedValue = (processorCount * 2).ToString(),
                Reason = $"Channel write workers ({effectiveWorkers}) is below processor count ({processorCount}). Consider increasing to at least 2x processor count for optimal parallelism.",
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }

    private void EvaluateBufferMemoryRule()
    {
        const string ruleId = "buffer-memory";
        if (IsRuleDisabled(ruleId)) return;

        // Rule: if channel write buffer is very large, it may cause GC pressure
        var currentBuffer = _brokerConfig.ChannelWriteBufferSize;
        if (currentBuffer > 100_000)
        {
            EmitRecommendation(new AutoTuningRecommendation
            {
                RuleId = ruleId,
                Description = "Reduce channel write buffer size to lower GC pressure",
                ConfigKey = "channel.write.buffer.size",
                CurrentValue = currentBuffer.ToString(),
                SuggestedValue = "50000",
                Reason = $"Channel write buffer size ({currentBuffer}) is very large. Large buffers increase GC pressure and memory usage. Consider reducing to 50,000 unless throughput requires it.",
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }

    private void EvaluatePartitionHotspotRule()
    {
        const string ruleId = "partition-hotspot";
        if (IsRuleDisabled(ruleId)) return;

        // Rule: if default partition count is 1, suggest more for distribution
        var partitionCount = _brokerConfig.DefaultNumPartitions;
        if (partitionCount == 1)
        {
            EmitRecommendation(new AutoTuningRecommendation
            {
                RuleId = ruleId,
                Description = "Increase default partition count to improve parallelism",
                ConfigKey = "num.partitions",
                CurrentValue = partitionCount.ToString(),
                SuggestedValue = "3",
                Reason = "Default partition count is 1. Multiple partitions enable parallel consumption and better load distribution across consumer group members.",
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }

    private void EvaluateIsrRule()
    {
        const string ruleId = "isr-health";
        if (IsRuleDisabled(ruleId)) return;

        // Rule: if min ISR = replication factor, there's no room for replica lag
        var minIsr = _brokerConfig.MinInSyncReplicas;
        var replicationFactor = _brokerConfig.DefaultReplicationFactor;

        if (replicationFactor > 1 && minIsr >= replicationFactor)
        {
            EmitRecommendation(new AutoTuningRecommendation
            {
                RuleId = ruleId,
                Description = "ISR configuration may cause availability issues",
                ConfigKey = "min.insync.replicas",
                CurrentValue = minIsr.ToString(),
                SuggestedValue = (replicationFactor - 1).ToString(),
                Reason = $"min.insync.replicas ({minIsr}) equals replication.factor ({replicationFactor}). If any replica goes down, producers with acks=all will be unable to produce. Set min.insync.replicas to replication.factor - 1.",
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }

    private void EvaluateLogSegmentRule()
    {
        const string ruleId = "log-segment-size";
        if (IsRuleDisabled(ruleId)) return;

        // Rule: if log segment size is very small, suggest increase
        var segmentBytes = _brokerConfig.LogSegmentBytes;
        if (segmentBytes < 100 * 1024 * 1024) // less than 100MB
        {
            EmitRecommendation(new AutoTuningRecommendation
            {
                RuleId = ruleId,
                Description = "Increase log segment size to reduce segment rotation frequency",
                ConfigKey = "log.segment.bytes",
                CurrentValue = segmentBytes.ToString(),
                SuggestedValue = (1073741824L).ToString(), // 1GB
                Reason = $"Log segment size ({segmentBytes / (1024 * 1024)}MB) is relatively small. Frequent segment rotation increases file descriptor usage and compaction overhead. Consider 1GB for production workloads.",
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }

    private bool IsRuleDisabled(string ruleId)
    {
        return _config.DisabledRules.Contains(ruleId, StringComparer.OrdinalIgnoreCase);
    }

    private void EmitRecommendation(AutoTuningRecommendation recommendation)
    {
        var shouldAutoApply = _config.Mode == AutoTuningMode.AutoApply;

        if (shouldAutoApply)
        {
            var error = _dynamicConfig.SetConfig(recommendation.ConfigKey, recommendation.SuggestedValue);
            if (error is not null)
            {
                _logger.LogWarning(
                    "Auto-tuning: failed to auto-apply {RuleId} ({ConfigKey}): {Error}",
                    recommendation.RuleId, recommendation.ConfigKey, error);

                // Store as suggestion instead
                _activeRecommendations[recommendation.RuleId] = recommendation;
            }
            else
            {
                var applied = recommendation with { WasAutoApplied = true };
                AddToHistory(applied);
                _logger.LogInformation(
                    "Auto-tuning: auto-applied {RuleId} - {ConfigKey} changed from {Old} to {New}",
                    recommendation.RuleId, recommendation.ConfigKey,
                    recommendation.CurrentValue, recommendation.SuggestedValue);
            }
        }
        else
        {
            // SuggestOnly or Mixed mode - store as active recommendation
            _activeRecommendations[recommendation.RuleId] = recommendation;
            AddToHistory(recommendation);
            _logger.LogInformation(
                "Auto-tuning suggestion: {RuleId} - {Description} ({ConfigKey}: {Current} -> {Suggested})",
                recommendation.RuleId, recommendation.Description,
                recommendation.ConfigKey, recommendation.CurrentValue, recommendation.SuggestedValue);
        }
    }

    private void AddToHistory(AutoTuningRecommendation recommendation)
    {
        _history.Enqueue(recommendation);

        // Trim history if it gets too large
        while (_history.Count > MaxHistorySize)
        {
            _history.TryDequeue(out _);
        }
    }
}

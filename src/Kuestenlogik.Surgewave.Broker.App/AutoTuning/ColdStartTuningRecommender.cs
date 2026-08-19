namespace Kuestenlogik.Surgewave.Broker.AutoTuning;

/// <summary>
/// Turns a closed <see cref="WorkloadProfile"/> into
/// <see cref="AutoTuningRecommendation"/>s grounded in *measured* traffic
/// shape — the workload-aware counterpart to <see cref="AutoTuningService"/>'s
/// static-config rules. Pure: no broker dependencies, fully testable.
/// </summary>
public static class ColdStartTuningRecommender
{
    /// <summary>
    /// Run the rule set against the supplied profile and current broker
    /// config snapshot, return the recommendations.
    /// </summary>
    /// <param name="profile">Built via <see cref="ColdStartWorkloadProfiler.BuildProfile"/>.</param>
    /// <param name="currentConfig">
    /// Snapshot of the relevant <c>broker.config</c> keys — passed in rather
    /// than read from <c>BrokerConfig</c> directly so the recommender stays
    /// pure. Unknown keys default to "0" / empty.
    /// </param>
    public static IReadOnlyList<AutoTuningRecommendation> Recommend(
        WorkloadProfile profile,
        ColdStartBrokerSnapshot currentConfig)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ArgumentNullException.ThrowIfNull(currentConfig);

        var now = DateTimeOffset.UtcNow;
        var list = new List<AutoTuningRecommendation>();

        EvaluatePartitionCount(profile, currentConfig, now, list);
        EvaluateBatchSize(profile, currentConfig, now, list);
        EvaluateReplicaFetchSize(profile, currentConfig, now, list);
        EvaluateLogSegmentSize(profile, currentConfig, now, list);

        return list;
    }

    private static void EvaluatePartitionCount(
        WorkloadProfile profile,
        ColdStartBrokerSnapshot config,
        DateTimeOffset now,
        List<AutoTuningRecommendation> list)
    {
        // Heuristic: if any topic averages > 10 MB/s sustained, the operator
        // is likely under-partitioned at the default. We never recommend
        // *fewer* partitions — that requires data movement.
        const long highBytesPerSecondPerTopic = 10L * 1024 * 1024;
        if (profile.ObservedFor.TotalSeconds <= 0) return;

        // Top topic by bytes — use it as the load representative.
        var topTopic = profile.Topics.OrderByDescending(t => t.TotalBytes).FirstOrDefault();
        if (topTopic is null) return;

        var topBps = topTopic.TotalBytes / (long)profile.ObservedFor.TotalSeconds;
        if (topBps < highBytesPerSecondPerTopic) return;
        if (config.DefaultNumPartitions >= 12) return; // already plenty

        var suggested = Math.Min(config.DefaultNumPartitions * 2, 12);
        list.Add(new AutoTuningRecommendation
        {
            RuleId = "cold-start.num-partitions",
            Description = "Hot topic detected — increase default partition count for parallelism",
            ConfigKey = "num.partitions",
            CurrentValue = config.DefaultNumPartitions.ToString(),
            SuggestedValue = suggested.ToString(),
            Reason = $"Topic '{topTopic.Topic}' averaged {topBps / (1024 * 1024)} MB/s over the cold-start window; with only {config.DefaultNumPartitions} partitions per topic, new topics of this shape will bottleneck on a single consumer thread.",
            Timestamp = now,
        });
    }

    private static void EvaluateBatchSize(
        WorkloadProfile profile,
        ColdStartBrokerSnapshot config,
        DateTimeOffset now,
        List<AutoTuningRecommendation> list)
    {
        // Heuristic: if total bytes-per-record is small (<512 B average) and
        // we saw real traffic, larger batch.size dramatically lowers
        // per-record network/syscall overhead. Capped at 256 KB.
        if (profile.TotalRecords < 10_000) return; // too quiet to recommend

        var avgRecordBytes = profile.TotalBytes / profile.TotalRecords;
        if (avgRecordBytes >= 512) return;
        if (config.ProducerBatchSizeBytes >= 262_144) return;

        var suggested = Math.Min(Math.Max(config.ProducerBatchSizeBytes * 4, 65_536), 262_144);
        list.Add(new AutoTuningRecommendation
        {
            RuleId = "cold-start.batch-size",
            Description = "Small records observed — enlarge producer batch.size for amortised overhead",
            ConfigKey = "producer.batch.size",
            CurrentValue = config.ProducerBatchSizeBytes.ToString(),
            SuggestedValue = suggested.ToString(),
            Reason = $"Average record size during cold-start was {avgRecordBytes} B across {profile.TotalRecords:N0} records. With a {config.ProducerBatchSizeBytes / 1024} KB batch, per-record syscall overhead dominates; raising to {suggested / 1024} KB amortises it.",
            Timestamp = now,
        });
    }

    private static void EvaluateReplicaFetchSize(
        WorkloadProfile profile,
        ColdStartBrokerSnapshot config,
        DateTimeOffset now,
        List<AutoTuningRecommendation> list)
    {
        // Heuristic: sustained high replication lag with default fetch size
        // → recommend doubling the fetch size (followers can't catch up
        // because each fetch round-trip only ships a tiny batch).
        if (profile.ReplicationLagSamples < 10) return;
        if (profile.MaxReplicationLagMs < 1000) return; // < 1 s lag — not a problem
        if (config.ReplicaFetchMaxBytes >= 4 * 1024 * 1024) return; // already big

        var suggested = Math.Min(config.ReplicaFetchMaxBytes * 2, 4 * 1024 * 1024);
        list.Add(new AutoTuningRecommendation
        {
            RuleId = "cold-start.replica-fetch-size",
            Description = "Followers lagging — enlarge replica.fetch.max.bytes",
            ConfigKey = "replica.fetch.max.bytes",
            CurrentValue = config.ReplicaFetchMaxBytes.ToString(),
            SuggestedValue = suggested.ToString(),
            Reason = $"Peak replication lag {profile.MaxReplicationLagMs} ms (avg {profile.AverageReplicationLagMs} ms over {profile.ReplicationLagSamples} samples) with fetch.max.bytes = {config.ReplicaFetchMaxBytes / 1024} KB. Doubling lets each fetch round-trip ship more data.",
            Timestamp = now,
        });
    }

    private static void EvaluateLogSegmentSize(
        WorkloadProfile profile,
        ColdStartBrokerSnapshot config,
        DateTimeOffset now,
        List<AutoTuningRecommendation> list)
    {
        // Heuristic: high peak throughput rolls log segments too fast at
        // the default 100 MB — compaction worker thrashes. Push to 1 GB.
        if (profile.PeakBytesPerSecond < 50L * 1024 * 1024) return; // < 50 MB/s — fine
        if (config.LogSegmentBytes >= 1024L * 1024 * 1024) return;

        list.Add(new AutoTuningRecommendation
        {
            RuleId = "cold-start.log-segment-size",
            Description = "Throughput rolls log segments too fast — increase log.segment.bytes",
            ConfigKey = "log.segment.bytes",
            CurrentValue = config.LogSegmentBytes.ToString(),
            SuggestedValue = (1024L * 1024 * 1024).ToString(),
            Reason = $"Peak throughput {profile.PeakBytesPerSecond / (1024 * 1024)} MB/s rolls a {config.LogSegmentBytes / (1024 * 1024)} MB segment every few seconds, thrashing the compaction worker and the OS page cache. 1 GB is the typical production setting.",
            Timestamp = now,
        });
    }
}

/// <summary>
/// Read-only snapshot of the broker-config keys the
/// <see cref="ColdStartTuningRecommender"/> inspects. Kept here so the
/// recommender doesn't take a hard dependency on the concrete
/// <c>BrokerConfig</c> type.
/// </summary>
public sealed record ColdStartBrokerSnapshot(
    int DefaultNumPartitions,
    int ProducerBatchSizeBytes,
    int ReplicaFetchMaxBytes,
    long LogSegmentBytes);

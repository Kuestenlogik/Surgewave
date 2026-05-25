using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Broker.AutoTuning;

/// <summary>
/// Thread-safe profiler that runs during a broker's "cold-start" observation
/// window — typically the first 24 hours after start — and accumulates the
/// per-topic Produce / Replication-lag stats needed for the
/// <see cref="ColdStartTuningRecommender"/> to derive
/// <see cref="AutoTuningRecommendation"/>s grounded in *actual* workload
/// rather than the broker's static defaults.
/// </summary>
/// <remarks>
/// <para>
/// The existing <see cref="AutoTuningService"/> rule set (batch-size,
/// compression, fetch-size, …) only inspects static <c>BrokerConfig</c>
/// values, so it can suggest "switch to lz4" forever even if the actual
/// payload is incompressible. The cold-start profiler fills that gap by
/// keeping a running snapshot of observed traffic shape, which the
/// recommender consumes once the window closes.
/// </para>
/// <para>
/// The hot-path API (<see cref="RecordProduce"/>, <see cref="RecordReplicationLag"/>)
/// is allocation-free under the steady state — a per-topic
/// <see cref="TopicProfile"/> is allocated once via
/// <see cref="ConcurrentDictionary{TKey,TValue}.GetOrAdd(TKey, Func{TKey, TValue})"/>
/// and updated via interlocked operations.
/// </para>
/// <para>
/// Time is injected via <see cref="TimeProvider"/> so tests can compress the
/// 24 h window without sleeping.
/// </para>
/// </remarks>
public sealed class ColdStartWorkloadProfiler
{
    private readonly TimeProvider _time;
    private readonly TimeSpan _observationWindow;
    private readonly DateTimeOffset _startedAt;
    private readonly ConcurrentDictionary<string, TopicProfile> _topics = new(StringComparer.Ordinal);

    private long _totalRecords;
    private long _totalBytes;
    private long _peakRecordsPerSecond;
    private long _peakBytesPerSecond;
    private long _currentSecondRecords;
    private long _currentSecondBytes;
    private long _currentSecondTimestamp;

    private long _replicationLagSamples;
    private long _replicationLagSumMs;
    private long _maxReplicationLagMs;

    public ColdStartWorkloadProfiler(TimeSpan? observationWindow = null, TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        _observationWindow = observationWindow ?? TimeSpan.FromHours(24);
        _startedAt = _time.GetUtcNow();
        _currentSecondTimestamp = _startedAt.ToUnixTimeSeconds();
    }

    /// <summary>When the profiler started — set at construction.</summary>
    public DateTimeOffset StartedAt => _startedAt;

    /// <summary>Configured observation window (default 24 h).</summary>
    public TimeSpan ObservationWindow => _observationWindow;

    /// <summary>True once the configured window has elapsed.</summary>
    public bool IsComplete => _time.GetUtcNow() - _startedAt >= _observationWindow;

    /// <summary>
    /// Record one Produce event. Cheap — single interlocked add per
    /// counter, plus an occasional per-second peak roll-over.
    /// </summary>
    public void RecordProduce(string topic, long recordCount, long byteCount)
    {
        if (string.IsNullOrEmpty(topic) || recordCount <= 0 || byteCount < 0)
        {
            return;
        }

        var topicProfile = _topics.GetOrAdd(topic, static t => new TopicProfile(t));
        topicProfile.Add(recordCount, byteCount);

        Interlocked.Add(ref _totalRecords, recordCount);
        Interlocked.Add(ref _totalBytes, byteCount);

        UpdatePerSecondPeak(recordCount, byteCount);
    }

    /// <summary>
    /// Record one replication-lag sample (e.g. taken every metrics-sweep).
    /// </summary>
    public void RecordReplicationLag(long lagMs)
    {
        if (lagMs < 0)
        {
            return;
        }
        Interlocked.Increment(ref _replicationLagSamples);
        Interlocked.Add(ref _replicationLagSumMs, lagMs);

        // CAS-update the max — no perf-critical path so the loop is cheap.
        long current;
        do
        {
            current = Interlocked.Read(ref _maxReplicationLagMs);
            if (lagMs <= current)
            {
                return;
            }
        } while (Interlocked.CompareExchange(ref _maxReplicationLagMs, lagMs, current) != current);
    }

    /// <summary>
    /// Snapshot the profile. Safe to call while observations continue —
    /// the returned record is a point-in-time view.
    /// </summary>
    public WorkloadProfile BuildProfile()
    {
        // Force any pending per-second window to roll over so the final
        // second's traffic is reflected in the peak.
        UpdatePerSecondPeak(0, 0);

        var observedFor = _time.GetUtcNow() - _startedAt;
        var topics = _topics.Values
            .Select(t => t.Snapshot())
            .OrderByDescending(t => t.TotalRecords)
            .ToList();
        var samples = Interlocked.Read(ref _replicationLagSamples);
        var avgLag = samples == 0 ? 0L : Interlocked.Read(ref _replicationLagSumMs) / samples;

        return new WorkloadProfile(
            StartedAt: _startedAt,
            ObservedFor: observedFor,
            IsComplete: IsComplete,
            TotalRecords: Interlocked.Read(ref _totalRecords),
            TotalBytes: Interlocked.Read(ref _totalBytes),
            TopicCardinality: topics.Count,
            PeakRecordsPerSecond: Interlocked.Read(ref _peakRecordsPerSecond),
            PeakBytesPerSecond: Interlocked.Read(ref _peakBytesPerSecond),
            AverageReplicationLagMs: avgLag,
            MaxReplicationLagMs: Interlocked.Read(ref _maxReplicationLagMs),
            ReplicationLagSamples: samples,
            Topics: topics);
    }

    private void UpdatePerSecondPeak(long records, long bytes)
    {
        var nowUnix = _time.GetUtcNow().ToUnixTimeSeconds();
        var currentTs = Interlocked.Read(ref _currentSecondTimestamp);

        if (nowUnix == currentTs)
        {
            // Same wall-second — just accumulate.
            Interlocked.Add(ref _currentSecondRecords, records);
            Interlocked.Add(ref _currentSecondBytes, bytes);
            return;
        }

        // Different wall-second — try to claim the roll-over. The thread
        // that wins compares the previous-second totals against the peaks.
        if (Interlocked.CompareExchange(ref _currentSecondTimestamp, nowUnix, currentTs) == currentTs)
        {
            var prevRecords = Interlocked.Exchange(ref _currentSecondRecords, records);
            var prevBytes = Interlocked.Exchange(ref _currentSecondBytes, bytes);
            BumpPeak(ref _peakRecordsPerSecond, prevRecords);
            BumpPeak(ref _peakBytesPerSecond, prevBytes);
        }
        else
        {
            // Another thread rolled over while we were checking — add to
            // whatever is now the current second.
            Interlocked.Add(ref _currentSecondRecords, records);
            Interlocked.Add(ref _currentSecondBytes, bytes);
        }
    }

    private static void BumpPeak(ref long peak, long candidate)
    {
        long current;
        do
        {
            current = Interlocked.Read(ref peak);
            if (candidate <= current)
            {
                return;
            }
        } while (Interlocked.CompareExchange(ref peak, candidate, current) != current);
    }

    private sealed class TopicProfile
    {
        private readonly string _topic;
        private long _records;
        private long _bytes;

        public TopicProfile(string topic) => _topic = topic;

        public void Add(long records, long bytes)
        {
            Interlocked.Add(ref _records, records);
            Interlocked.Add(ref _bytes, bytes);
        }

        public TopicWorkloadSnapshot Snapshot() => new(
            Topic: _topic,
            TotalRecords: Interlocked.Read(ref _records),
            TotalBytes: Interlocked.Read(ref _bytes));
    }
}

/// <summary>
/// Point-in-time snapshot of accumulated cold-start workload metrics.
/// Consumed by <see cref="ColdStartTuningRecommender"/>.
/// </summary>
public sealed record WorkloadProfile(
    DateTimeOffset StartedAt,
    TimeSpan ObservedFor,
    bool IsComplete,
    long TotalRecords,
    long TotalBytes,
    int TopicCardinality,
    long PeakRecordsPerSecond,
    long PeakBytesPerSecond,
    long AverageReplicationLagMs,
    long MaxReplicationLagMs,
    long ReplicationLagSamples,
    IReadOnlyList<TopicWorkloadSnapshot> Topics);

/// <summary>Per-topic stats inside <see cref="WorkloadProfile.Topics"/>.</summary>
public sealed record TopicWorkloadSnapshot(string Topic, long TotalRecords, long TotalBytes);

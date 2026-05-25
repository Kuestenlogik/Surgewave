using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Kuestenlogik.Surgewave.Streams.EventTime;
using Kuestenlogik.Surgewave.Streams.Processors;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Processing context available to processors and transformers.
/// </summary>
public sealed class ProcessorContext
{
    private readonly ConcurrentDictionary<string, IStateStore> _stateStores = new();
    private readonly List<Action<byte[], byte[], long>> _forwards = new();
    private readonly List<ScheduledPunctuation> _punctuations = new();
    private long _lastStreamTime;
    private long _lastWallClockCheck;

    public string ApplicationId { get; }
    public string? TaskId { get; internal set; }
    public string? Topic { get; internal set; }
    public int Partition { get; internal set; }
    public long Offset { get; internal set; }
    public long Timestamp { get; internal set; }
    public IReadOnlyDictionary<string, byte[]>? Headers { get; internal set; }
    public StreamsConfig Config { get; }
    public StreamsMetrics Metrics { get; }
    public ILogger Logger { get; }
    public Activity? CurrentActivity { get; internal set; }

    public ProcessorContext(StreamsConfig config, StreamsMetrics metrics, ILogger logger)
    {
        ApplicationId = config.ApplicationId;
        Config = config;
        Metrics = metrics;
        Logger = logger;
        _lastWallClockCheck = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public void RegisterStateStore(IStateStore store)
    {
        _stateStores[store.Name] = store;
        store.Init(this);
    }

    public TStore? GetStateStore<TStore>(string name) where TStore : class, IStateStore
    {
        if (_stateStores.TryGetValue(name, out var store) && store is TStore typedStore)
        {
            return typedStore;
        }
        return null;
    }

    public void Forward<TKey, TValue>(TKey key, TValue value, ISerde<TKey> keySerde, ISerde<TValue> valueSerde)
    {
        var keyBytes = keySerde.Serialize(key);
        var valueBytes = valueSerde.Serialize(value);
        foreach (var forward in _forwards)
        {
            forward(keyBytes, valueBytes, Timestamp);
        }
    }

    internal void AddForwardHandler(Action<byte[], byte[], long> handler)
    {
        _forwards.Add(handler);
    }

    public void Commit()
    {
        foreach (var store in _stateStores.Values)
        {
            store.Flush();
        }
    }

    /// <summary>
    /// Gets the current watermark timestamp. Returns Watermark.None if no watermark has been set.
    /// </summary>
    public Watermark CurrentWatermark { get; private set; } = Watermark.None;

    /// <summary>
    /// Updates the watermark to the given value (only advances, never goes backward).
    /// </summary>
    public void UpdateWatermark(Watermark watermark)
    {
        if (watermark > CurrentWatermark)
            CurrentWatermark = watermark;
    }

    public long CurrentStreamTimeMs() => Timestamp;

    public long CurrentSystemTimeMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>
    /// Schedule a punctuation callback to be invoked periodically.
    /// </summary>
    /// <param name="interval">The interval between invocations.</param>
    /// <param name="type">The type of punctuation (stream time or wall clock time).</param>
    /// <param name="callback">The callback to invoke with the current timestamp.</param>
    /// <returns>A cancellable handle for the scheduled punctuation.</returns>
    public ICancellable Schedule(TimeSpan interval, PunctuationType type, Action<long> callback)
    {
        var intervalMs = (long)interval.TotalMilliseconds;
        var now = type == PunctuationType.WallClockTime
            ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            : _lastStreamTime;

        var punctuation = new ScheduledPunctuation
        {
            IntervalMs = intervalMs,
            Type = type,
            Callback = callback,
            NextScheduledTimeMs = now + intervalMs,
            IsCancelled = false
        };

        _punctuations.Add(punctuation);
        Logger.LogDebug("Scheduled {Type} punctuation with interval {Interval}ms", type, intervalMs);

        return new PunctuationCancellable(punctuation, _punctuations);
    }

    /// <summary>
    /// Called after processing each record to potentially fire stream-time punctuations.
    /// </summary>
    internal void MaybeFireStreamTimePunctuations(long streamTime)
    {
        if (streamTime > _lastStreamTime)
        {
            _lastStreamTime = streamTime;
        }

        for (var i = 0; i < _punctuations.Count; i++)
        {
            var p = _punctuations[i];
            if (p.IsCancelled || p.Type != PunctuationType.StreamTime)
                continue;

            while (p.NextScheduledTimeMs <= _lastStreamTime)
            {
                try
                {
                    p.Callback(p.NextScheduledTimeMs);
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Error in stream-time punctuation callback");
                }
                p.NextScheduledTimeMs += p.IntervalMs;
            }
        }
    }

    /// <summary>
    /// Called periodically to fire wall-clock-time punctuations.
    /// </summary>
    internal void MaybeFireWallClockTimePunctuations()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        for (var i = 0; i < _punctuations.Count; i++)
        {
            var p = _punctuations[i];
            if (p.IsCancelled || p.Type != PunctuationType.WallClockTime)
                continue;

            while (p.NextScheduledTimeMs <= now)
            {
                try
                {
                    p.Callback(p.NextScheduledTimeMs);
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Error in wall-clock-time punctuation callback");
                }
                p.NextScheduledTimeMs += p.IntervalMs;
            }
        }

        _lastWallClockCheck = now;
    }

    /// <summary>
    /// Remove all cancelled punctuations.
    /// </summary>
    internal void CleanupCancelledPunctuations()
    {
        _punctuations.RemoveAll(p => p.IsCancelled);
    }
}

/// <summary>
/// Represents a scheduled punctuation.
/// </summary>
internal sealed class ScheduledPunctuation
{
    public required long IntervalMs { get; init; }
    public required PunctuationType Type { get; init; }
    public required Action<long> Callback { get; init; }
    public long NextScheduledTimeMs { get; set; }
    public bool IsCancelled { get; set; }
}

/// <summary>
/// Implementation of cancellable for punctuations.
/// </summary>
internal sealed class PunctuationCancellable : ICancellable
{
    private readonly ScheduledPunctuation _punctuation;
    private readonly List<ScheduledPunctuation> _punctuations;

    public PunctuationCancellable(ScheduledPunctuation punctuation, List<ScheduledPunctuation> punctuations)
    {
        _punctuation = punctuation;
        _punctuations = punctuations;
    }

    public void Cancel()
    {
        _punctuation.IsCancelled = true;
    }
}

using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Broker.Quotas;

/// <summary>
/// Per-client bandwidth tracking using sliding window counters.
/// Thread-safe for concurrent produce/consume recording and checking.
/// </summary>
public sealed class BandwidthTracker
{
    private readonly ConcurrentDictionary<string, ClientBandwidthState> _clients = new();
    private readonly int _windowMs;

    /// <summary>
    /// Creates a new bandwidth tracker.
    /// </summary>
    /// <param name="windowMs">Sliding window duration in milliseconds for rate measurement.</param>
    public BandwidthTracker(int windowMs = 1000)
    {
        _windowMs = windowMs;
    }

    /// <summary>
    /// Record bytes produced by a client.
    /// </summary>
    public void RecordProduce(string clientId, long bytes)
    {
        var state = GetOrCreate(clientId);
        state.ProduceCounter.Record(bytes);
        state.LastActivityAt = DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// Record bytes consumed by a client.
    /// </summary>
    public void RecordConsume(string clientId, long bytes)
    {
        var state = GetOrCreate(clientId);
        state.ConsumeCounter.Record(bytes);
        state.LastActivityAt = DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// Check if a produce request should be throttled.
    /// </summary>
    /// <param name="clientId">Client identifier.</param>
    /// <param name="bytes">Number of bytes to produce.</param>
    /// <param name="limitBytesPerSec">The produce limit in bytes/sec. 0 = unlimited.</param>
    /// <param name="delayFactor">Backoff multiplier for delay calculation.</param>
    /// <returns>Throttle result indicating whether to delay.</returns>
    public ThrottleResult CheckProduce(string clientId, long bytes, long limitBytesPerSec, double delayFactor)
    {
        if (limitBytesPerSec <= 0)
            return new ThrottleResult(false, null, 0, 0);

        var clientState = GetOrCreate(clientId);
        return CheckRate(clientState.ProduceCounter, bytes, limitBytesPerSec, delayFactor);
    }

    /// <summary>
    /// Check if a consume request should be throttled.
    /// </summary>
    /// <param name="clientId">Client identifier.</param>
    /// <param name="bytes">Number of bytes to consume.</param>
    /// <param name="limitBytesPerSec">The consume limit in bytes/sec. 0 = unlimited.</param>
    /// <param name="delayFactor">Backoff multiplier for delay calculation.</param>
    /// <returns>Throttle result indicating whether to delay.</returns>
    public ThrottleResult CheckConsume(string clientId, long bytes, long limitBytesPerSec, double delayFactor)
    {
        if (limitBytesPerSec <= 0)
            return new ThrottleResult(false, null, 0, 0);

        var clientState = GetOrCreate(clientId);
        return CheckRate(clientState.ConsumeCounter, bytes, limitBytesPerSec, delayFactor);
    }

    private static ThrottleResult CheckRate(SlidingWindowCounter counter, long bytes, long limitBytesPerSec, double delayFactor)
    {
        if (limitBytesPerSec <= 0)
        {
            return new ThrottleResult(false, null, 0, 0);
        }

        var currentRate = counter.GetCurrentRate();

        if (currentRate + bytes <= limitBytesPerSec)
        {
            return new ThrottleResult(false, null, currentRate, limitBytesPerSec);
        }

        // Calculate delay: how long to wait for the excess to drain
        var excess = currentRate + bytes - limitBytesPerSec;
        var delaySeconds = (double)excess / limitBytesPerSec * delayFactor;
        var delay = TimeSpan.FromSeconds(Math.Min(delaySeconds, 30.0));

        return new ThrottleResult(true, delay, currentRate, limitBytesPerSec);
    }

    /// <summary>
    /// Get current bandwidth usage for a specific client.
    /// </summary>
    public BandwidthUsage? GetUsage(string clientId, long produceLimitBps = 0, long consumeLimitBps = 0)
    {
        if (!_clients.TryGetValue(clientId, out var state))
            return null;

        return BuildUsage(clientId, state, produceLimitBps, consumeLimitBps);
    }

    /// <summary>
    /// Get current bandwidth usage for all tracked clients.
    /// </summary>
    public IReadOnlyList<BandwidthUsage> GetAllUsage(
        Func<string, (long produce, long consume)> limitResolver)
    {
        var result = new List<BandwidthUsage>();

        foreach (var (clientId, state) in _clients)
        {
            var (produceLimitBps, consumeLimitBps) = limitResolver(clientId);
            result.Add(BuildUsage(clientId, state, produceLimitBps, consumeLimitBps));
        }

        return result;
    }

    /// <summary>
    /// Remove tracking state for clients that have been inactive for longer than the specified duration.
    /// </summary>
    public int CleanupInactive(TimeSpan maxIdleTime)
    {
        var cutoff = DateTimeOffset.UtcNow - maxIdleTime;
        var removed = 0;

        foreach (var (clientId, state) in _clients)
        {
            if (state.LastActivityAt < cutoff)
            {
                if (_clients.TryRemove(clientId, out _))
                    removed++;
            }
        }

        return removed;
    }

    private ClientBandwidthState GetOrCreate(string clientId)
    {
        return _clients.GetOrAdd(clientId, _ => new ClientBandwidthState(_windowMs));
    }

    private static BandwidthUsage BuildUsage(string clientId, ClientBandwidthState state, long produceLimitBps, long consumeLimitBps)
    {
        var produceRate = state.ProduceCounter.GetCurrentRate();
        var consumeRate = state.ConsumeCounter.GetCurrentRate();

        return new BandwidthUsage
        {
            ClientId = clientId,
            ProduceBytesPerSec = produceRate,
            ConsumeBytesPerSec = consumeRate,
            ProduceLimitBytesPerSec = produceLimitBps,
            ConsumeLimitBytesPerSec = consumeLimitBps,
            ProduceUtilizationPercent = produceLimitBps > 0 ? Math.Min(100.0, (double)produceRate / produceLimitBps * 100) : 0,
            ConsumeUtilizationPercent = consumeLimitBps > 0 ? Math.Min(100.0, (double)consumeRate / consumeLimitBps * 100) : 0,
            IsThrottled = (produceLimitBps > 0 && produceRate > produceLimitBps) ||
                          (consumeLimitBps > 0 && consumeRate > consumeLimitBps),
            LastActivityAt = state.LastActivityAt
        };
    }

    private sealed class ClientBandwidthState
    {
        public SlidingWindowCounter ProduceCounter { get; }
        public SlidingWindowCounter ConsumeCounter { get; }
        public DateTimeOffset LastActivityAt { get; set; } = DateTimeOffset.UtcNow;

        public ClientBandwidthState(int windowMs)
        {
            ProduceCounter = new SlidingWindowCounter(windowMs);
            ConsumeCounter = new SlidingWindowCounter(windowMs);
        }
    }
}

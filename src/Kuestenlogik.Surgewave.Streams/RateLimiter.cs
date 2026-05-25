using System.Diagnostics;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Lock-free token bucket rate limiter using Interlocked operations.
/// </summary>
public sealed class RateLimiter
{
    private readonly long _tokensPerSecond;
    private long _availableTokens;
    private long _lastRefillTicks;
    private long _throttleCount;
    private long _totalWaitTimeMs;

    public long ThrottleCount => Interlocked.Read(ref _throttleCount);
    public long TotalWaitTimeMs => Interlocked.Read(ref _totalWaitTimeMs);
    public long AvailableTokens => Interlocked.Read(ref _availableTokens);
    public bool IsUnlimited => _tokensPerSecond <= 0;

    public RateLimiter(long tokensPerSecond)
    {
        _tokensPerSecond = tokensPerSecond;
        _availableTokens = tokensPerSecond;
        _lastRefillTicks = Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// Tries to consume tokens from the bucket using lock-free CAS loop.
    /// </summary>
    public bool TryConsume(long tokens)
    {
        if (IsUnlimited)
            return true;

        RefillInternal();

        while (true)
        {
            var current = Interlocked.Read(ref _availableTokens);
            if (current < tokens)
            {
                Interlocked.Increment(ref _throttleCount);
                return false;
            }

            if (Interlocked.CompareExchange(ref _availableTokens, current - tokens, current) == current)
                return true;
        }
    }

    /// <summary>
    /// Calculates how long to wait before tokens become available.
    /// </summary>
    public long CalculateWaitTimeMs(long tokens)
    {
        if (IsUnlimited)
            return 0;

        RefillInternal();

        var current = Interlocked.Read(ref _availableTokens);
        if (current >= tokens)
            return 0;

        var deficit = tokens - current;
        var waitMs = Math.Max(1, deficit * 1000 / _tokensPerSecond);
        Interlocked.Add(ref _totalWaitTimeMs, waitMs);
        return waitMs;
    }

    /// <summary>
    /// Forces a refill of the token bucket based on elapsed time.
    /// </summary>
    public void Refill()
    {
        RefillInternal();
    }

    private void RefillInternal()
    {
        var now = Stopwatch.GetTimestamp();
        var lastTicks = Interlocked.Read(ref _lastRefillTicks);
        var elapsedMs = (now - lastTicks) * 1000L / Stopwatch.Frequency;

        if (elapsedMs <= 0)
            return;

        var newTokens = elapsedMs * _tokensPerSecond / 1000;
        if (newTokens <= 0)
            return;

        // CAS loop for refill
        if (Interlocked.CompareExchange(ref _lastRefillTicks, now, lastTicks) == lastTicks)
        {
            while (true)
            {
                var current = Interlocked.Read(ref _availableTokens);
                var updated = Math.Min(_tokensPerSecond, current + newTokens);
                if (Interlocked.CompareExchange(ref _availableTokens, updated, current) == current)
                    break;
            }
        }
    }
}

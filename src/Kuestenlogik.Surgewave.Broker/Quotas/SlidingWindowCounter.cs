using System.Threading;

namespace Kuestenlogik.Surgewave.Broker.Quotas;

/// <summary>
/// Efficient sliding window byte counter using a fixed-size bucket array.
/// Each bucket tracks bytes recorded during a time slice (e.g., 100ms).
/// The sum of all buckets within the window yields the current rate.
/// Thread-safe via Interlocked operations.
/// </summary>
public sealed class SlidingWindowCounter
{
    private readonly long[] _buckets;
    private readonly int _bucketCount;
    private readonly long _bucketDurationTicks;
    private readonly long _windowDurationTicks;
    private long _currentBucketIndex;
    private long _lastBucketTimestamp;

    /// <summary>
    /// Creates a new sliding window counter.
    /// </summary>
    /// <param name="windowMs">Total window duration in milliseconds (default 1000).</param>
    /// <param name="bucketCount">Number of buckets to divide the window into (default 10).</param>
    public SlidingWindowCounter(int windowMs = 1000, int bucketCount = 10)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(windowMs, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(bucketCount, 0);

        _bucketCount = bucketCount;
        _buckets = new long[bucketCount];
        _windowDurationTicks = TimeSpan.FromMilliseconds(windowMs).Ticks;
        _bucketDurationTicks = _windowDurationTicks / bucketCount;
        _lastBucketTimestamp = GetCurrentTimestamp();
    }

    /// <summary>
    /// Record bytes in the current time bucket.
    /// </summary>
    public void Record(long bytes)
    {
        AdvanceBuckets();
        var index = Interlocked.Read(ref _currentBucketIndex) % _bucketCount;
        Interlocked.Add(ref _buckets[index], bytes);
    }

    /// <summary>
    /// Get the current rate in bytes per second across the sliding window.
    /// </summary>
    public long GetCurrentRate()
    {
        AdvanceBuckets();

        long total = 0;
        for (int i = 0; i < _bucketCount; i++)
        {
            total += Interlocked.Read(ref _buckets[i]);
        }

        // Convert from bytes/window to bytes/second
        var windowSeconds = (double)_windowDurationTicks / TimeSpan.TicksPerSecond;
        return (long)(total / windowSeconds);
    }

    /// <summary>
    /// Get the total bytes recorded in the current window (not rate-adjusted).
    /// </summary>
    public long GetWindowTotal()
    {
        AdvanceBuckets();

        long total = 0;
        for (int i = 0; i < _bucketCount; i++)
        {
            total += Interlocked.Read(ref _buckets[i]);
        }

        return total;
    }

    /// <summary>
    /// Reset all buckets to zero.
    /// </summary>
    public void Reset()
    {
        for (int i = 0; i < _bucketCount; i++)
        {
            Interlocked.Exchange(ref _buckets[i], 0);
        }

        Interlocked.Exchange(ref _lastBucketTimestamp, GetCurrentTimestamp());
    }

    private void AdvanceBuckets()
    {
        var now = GetCurrentTimestamp();
        var lastTimestamp = Interlocked.Read(ref _lastBucketTimestamp);
        var elapsed = now - lastTimestamp;

        if (elapsed < _bucketDurationTicks)
            return;

        // Calculate how many buckets to advance
        var bucketsToAdvance = (int)(elapsed / _bucketDurationTicks);

        if (bucketsToAdvance <= 0)
            return;

        // Try to claim the advance via CAS
        if (Interlocked.CompareExchange(ref _lastBucketTimestamp, now, lastTimestamp) != lastTimestamp)
            return; // Another thread already advanced

        // Clear old buckets
        var currentIdx = Interlocked.Read(ref _currentBucketIndex);
        var toClear = Math.Min(bucketsToAdvance, _bucketCount);

        for (int i = 1; i <= toClear; i++)
        {
            var clearIdx = (currentIdx + i) % _bucketCount;
            Interlocked.Exchange(ref _buckets[clearIdx], 0);
        }

        Interlocked.Add(ref _currentBucketIndex, bucketsToAdvance);
    }

    private static long GetCurrentTimestamp() => DateTime.UtcNow.Ticks;
}

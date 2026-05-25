using System.Diagnostics;

namespace Kuestenlogik.Surgewave.Streams.Resilience;

/// <summary>
/// Thread-safe circuit breaker that prevents cascading failures by fast-failing
/// requests when a failure threshold is exceeded, and allowing recovery probes
/// after a configurable reset timeout.
/// </summary>
public sealed class CircuitBreaker
{
    private readonly int _failureThreshold;
    private readonly TimeSpan _resetTimeout;

    // State is stored as int so Interlocked can be used:
    // 0 = Closed, 1 = Open, 2 = HalfOpen
    private int _state;
    private int _failureCount;
    private long _openedAtTimestamp;   // Stopwatch.GetTimestamp() when the breaker was tripped

    /// <summary>
    /// Initialises the circuit breaker with the given thresholds.
    /// </summary>
    /// <param name="failureThreshold">Number of consecutive failures before opening the circuit (default 5).</param>
    /// <param name="resetTimeout">Time to wait in the Open state before attempting recovery (default 30 s).</param>
    public CircuitBreaker(int failureThreshold = 5, TimeSpan resetTimeout = default)
    {
        _failureThreshold = failureThreshold;
        _resetTimeout = resetTimeout == default ? TimeSpan.FromSeconds(30) : resetTimeout;
    }

    /// <summary>Gets the current state of the circuit breaker.</summary>
    public CircuitBreakerState State => (CircuitBreakerState)Volatile.Read(ref _state);

    /// <summary>
    /// Records a successful operation.
    /// Resets the failure counter and transitions HalfOpen → Closed.
    /// </summary>
    public void RecordSuccess()
    {
        Interlocked.Exchange(ref _failureCount, 0);

        // Only transition if we are in HalfOpen; Closed stays Closed.
        Interlocked.CompareExchange(ref _state,
            (int)CircuitBreakerState.Closed,
            (int)CircuitBreakerState.HalfOpen);
    }

    /// <summary>
    /// Records a failed operation.
    /// Increments the failure counter and transitions Closed → Open when the
    /// threshold is reached.
    /// </summary>
    public void RecordFailure()
    {
        var failures = Interlocked.Increment(ref _failureCount);

        if (failures >= _failureThreshold)
        {
            // Trip the breaker: Closed or HalfOpen → Open
            var previous = Interlocked.Exchange(ref _state, (int)CircuitBreakerState.Open);
            if (previous != (int)CircuitBreakerState.Open)
            {
                // Record when we opened so we can compute the reset timeout
                Interlocked.Exchange(ref _openedAtTimestamp, Stopwatch.GetTimestamp());
            }
        }
    }

    /// <summary>
    /// Determines whether a request should be allowed through.
    /// <list type="bullet">
    ///   <item>Closed → <c>true</c></item>
    ///   <item>HalfOpen → <c>true</c> (single probe)</item>
    ///   <item>Open + timeout not elapsed → <c>false</c></item>
    ///   <item>Open + timeout elapsed → transitions to HalfOpen, returns <c>true</c></item>
    /// </list>
    /// </summary>
    public bool AllowRequest()
    {
        var current = Volatile.Read(ref _state);

        if (current == (int)CircuitBreakerState.Closed ||
            current == (int)CircuitBreakerState.HalfOpen)
            return true;

        // Open: check whether the reset timeout has elapsed
        var openedAt = Volatile.Read(ref _openedAtTimestamp);
        var elapsed = Stopwatch.GetElapsedTime(openedAt);

        if (elapsed < _resetTimeout)
            return false;

        // Attempt to transition Open → HalfOpen (only one thread wins)
        var won = Interlocked.CompareExchange(
            ref _state,
            (int)CircuitBreakerState.HalfOpen,
            (int)CircuitBreakerState.Open);

        return won == (int)CircuitBreakerState.Open;
    }
}

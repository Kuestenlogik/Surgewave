namespace Kuestenlogik.Surgewave.Streams.Resilience;

/// <summary>
/// Configuration for retry behavior in streams processing.
/// </summary>
public sealed class StreamsRetryConfig
{
    public bool Enabled { get; init; }
    public int MaxRetries { get; init; } = 3;
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(5);
    public BackoffStrategy BackoffStrategy { get; init; } = BackoffStrategy.ExponentialWithJitter;
    public Func<Exception, bool>? ShouldRetry { get; init; }

    // ── Circuit-breaker settings ──────────────────────────────────────────
    /// <summary>Activates the circuit breaker (default: false).</summary>
    public bool EnableCircuitBreaker { get; init; }

    /// <summary>Number of consecutive failures that trip the circuit breaker (default: 5).</summary>
    public int CircuitBreakerThreshold { get; init; } = 5;

    /// <summary>How long the breaker stays Open before probing recovery (default: 30 s).</summary>
    public TimeSpan CircuitBreakerResetTimeout { get; init; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// Backoff strategy for retry delays.
/// </summary>
public enum BackoffStrategy
{
    Fixed,
    Exponential,
    ExponentialWithJitter,
    Linear
}

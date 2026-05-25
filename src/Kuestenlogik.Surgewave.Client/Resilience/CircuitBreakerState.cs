namespace Kuestenlogik.Surgewave.Client.Resilience;

/// <summary>
/// Represents the state of a circuit breaker.
/// </summary>
public enum CircuitBreakerState
{
    /// <summary>
    /// Circuit is closed - requests flow through normally.
    /// </summary>
    Closed,

    /// <summary>
    /// Circuit is open - requests are immediately rejected.
    /// </summary>
    Open,

    /// <summary>
    /// Circuit is half-open - a single test request is allowed through.
    /// </summary>
    HalfOpen
}

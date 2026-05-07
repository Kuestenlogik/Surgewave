namespace Kuestenlogik.Surgewave.Streams.Resilience;

/// <summary>
/// Represents the current state of a circuit breaker.
/// </summary>
public enum CircuitBreakerState
{
    /// <summary>Normal operation; requests are allowed through.</summary>
    Closed,

    /// <summary>Failure threshold exceeded; requests are rejected immediately.</summary>
    Open,

    /// <summary>Reset timeout elapsed; a single probe request is allowed to test recovery.</summary>
    HalfOpen
}

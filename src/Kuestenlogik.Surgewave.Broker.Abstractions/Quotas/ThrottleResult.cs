namespace Kuestenlogik.Surgewave.Broker.Quotas;

/// <summary>
/// Result of a bandwidth throttle check indicating whether a request should be delayed.
/// </summary>
/// <param name="Throttled">True if the client is over its bandwidth quota.</param>
/// <param name="Delay">How long the client should wait before retrying. Null if not throttled.</param>
/// <param name="CurrentBytesPerSec">Current measured bandwidth usage in bytes/sec.</param>
/// <param name="LimitBytesPerSec">The configured limit in bytes/sec. 0 = unlimited.</param>
public sealed record ThrottleResult(
    bool Throttled,
    TimeSpan? Delay,
    long CurrentBytesPerSec,
    long LimitBytesPerSec);

namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

/// <summary>
/// Result of a subscription attempt returned by <see cref="SubscriptionManager.Subscribe"/>.
/// </summary>
public sealed record SubscriptionResult
{
    /// <summary>
    /// Error code. 0 = success, 50 = ExclusiveAlreadyBound, 51 = TypeMismatch.
    /// </summary>
    public required int ErrorCode { get; init; }

    /// <summary>
    /// Human-readable error message when ErrorCode is non-zero.
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// True if this consumer is the active one (receiving messages).
    /// For Exclusive: always true on success.
    /// For Shared/KeyShared: always true.
    /// For Failover: true only for the active consumer, false for standby.
    /// For Standard: always true.
    /// </summary>
    public required bool IsActive { get; init; }
}

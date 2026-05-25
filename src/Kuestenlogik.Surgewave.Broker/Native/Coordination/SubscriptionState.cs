namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

/// <summary>
/// Internal mutable state for a single subscription.
/// Tracks consumers, dispatch counters, and hash ring assignments.
/// </summary>
internal sealed class SubscriptionState
{
    /// <summary>
    /// Subscription name (unique identifier).
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// The subscription type governing dispatch behavior.
    /// </summary>
    public required SubscriptionType Type { get; init; }

    /// <summary>
    /// Consumers ordered by join time. First element is the earliest joiner.
    /// </summary>
    public List<string> Consumers { get; } = [];

    /// <summary>
    /// For Failover: the currently active consumer receiving messages.
    /// </summary>
    public string? ActiveConsumer { get; set; }

    /// <summary>
    /// For Shared: atomic round-robin dispatch counter.
    /// </summary>
    public long DispatchCounter { get; set; }

    /// <summary>
    /// For KeyShared: hash range assignments mapping consumer ID to (Start, End) inclusive range
    /// within 0..65535 virtual hash slots.
    /// </summary>
    public Dictionary<string, (int Start, int End)> HashRanges { get; } = [];
}

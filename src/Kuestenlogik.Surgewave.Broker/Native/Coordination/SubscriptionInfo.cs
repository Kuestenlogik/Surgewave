namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

/// <summary>
/// Public read-only information about a subscription.
/// Returned by <see cref="SubscriptionManager.DescribeSubscription"/> and <see cref="SubscriptionManager.ListSubscriptions"/>.
/// </summary>
public sealed record SubscriptionInfo
{
    /// <summary>
    /// Subscription name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// The subscription type.
    /// </summary>
    public required SubscriptionType Type { get; init; }

    /// <summary>
    /// Number of consumers currently in the subscription.
    /// </summary>
    public required int ConsumerCount { get; init; }

    /// <summary>
    /// For Failover: the currently active consumer. Null for other types.
    /// </summary>
    public string? ActiveConsumer { get; init; }

    /// <summary>
    /// List of consumer IDs in join order.
    /// </summary>
    public required List<string> Consumers { get; init; }
}

namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

/// <summary>
/// Defines the subscription mode for consumer groups.
/// Extends standard Kafka consumer group semantics with Pulsar-inspired subscription types.
/// </summary>
public enum SubscriptionType
{
    /// <summary>
    /// Standard Kafka-style: partitions assigned to consumers in group. Default.
    /// </summary>
    Standard = 0,

    /// <summary>
    /// Only one consumer allowed per subscription. Additional consumers rejected.
    /// </summary>
    Exclusive = 1,

    /// <summary>
    /// Round-robin message-level dispatch to multiple consumers (no partition ordering).
    /// </summary>
    Shared = 2,

    /// <summary>
    /// One active consumer, others standby. Auto-failover on consumer death.
    /// </summary>
    Failover = 3,

    /// <summary>
    /// Like Shared but messages with same key always go to same consumer (key ordering preserved).
    /// </summary>
    KeyShared = 4
}

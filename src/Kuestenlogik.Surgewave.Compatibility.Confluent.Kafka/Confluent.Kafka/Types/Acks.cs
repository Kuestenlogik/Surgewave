namespace Confluent.Kafka;

/// <summary>
/// Acknowledgment level for produce requests.
/// </summary>
public enum Acks
{
    /// <summary>
    /// No acknowledgment required. The producer will not wait for any acknowledgment.
    /// This provides the lowest latency but no delivery guarantee.
    /// </summary>
    None = 0,

    /// <summary>
    /// Leader acknowledgment. The producer will wait for the leader to acknowledge.
    /// This provides a balance between latency and durability.
    /// </summary>
    Leader = 1,

    /// <summary>
    /// All in-sync replicas must acknowledge. The producer will wait for all in-sync
    /// replicas to acknowledge. This provides the strongest durability guarantee.
    /// </summary>
    All = -1
}

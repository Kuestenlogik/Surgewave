namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Neutral input for a broker heartbeat RPC (#59 b5). Carries only the values the wire
/// request is built from; the Kafka-DTO construction stays inside the concrete
/// <see cref="BrokerLifecycleManager"/>.
/// </summary>
/// <param name="BrokerId">The heartbeating broker's id.</param>
/// <param name="BrokerEpoch">The broker epoch assigned during registration.</param>
/// <param name="CurrentMetadataOffset">The highest metadata offset reached by this broker.</param>
/// <param name="WantFence">True if the broker wants to be fenced.</param>
/// <param name="WantShutDown">True if the broker wants to initiate controlled shutdown.</param>
public sealed record BrokerHeartbeatInput(
    int BrokerId,
    long BrokerEpoch,
    long CurrentMetadataOffset,
    bool WantFence,
    bool WantShutDown);

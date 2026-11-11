namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Neutral input for a broker heartbeat RPC (#59 b5). Carries only the values the wire
/// request is built from; the frame construction stays inside the concrete
/// <see cref="IBrokerLifecycleRpc"/> implementation.
/// </summary>
/// <param name="BrokerId">The heartbeating broker's id.</param>
/// <param name="BrokerEpoch">The broker epoch assigned during registration.</param>
/// <param name="CurrentMetadataOffset">The highest metadata offset reached by this broker.</param>
/// <param name="WantFence">True if the broker wants to be fenced.</param>
/// <param name="WantShutDown">True if the broker wants to initiate controlled shutdown.</param>
/// <param name="OfflineLogDirs">
/// The ids of the broker's log directories that have failed (#162) — Kafka's <c>OfflineLogDirs</c> (KIP-858), which
/// a broker sends with every heartbeat once a directory is offline. Both wires carry it into this one record, so the
/// controller's answer does not depend on the wire it came over.
/// </param>
public sealed record BrokerHeartbeatInput(
    int BrokerId,
    long BrokerEpoch,
    long CurrentMetadataOffset,
    bool WantFence,
    bool WantShutDown,
    IReadOnlyList<Guid> OfflineLogDirs);

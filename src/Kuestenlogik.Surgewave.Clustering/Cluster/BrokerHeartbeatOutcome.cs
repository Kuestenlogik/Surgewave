using Kuestenlogik.Surgewave.Clustering.Replication;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Neutral outcome of a broker heartbeat RPC (#59 b5). Exposes only the fields the caller
/// reads to drive the lifecycle state machine, plus a protocol-neutral status.
/// </summary>
/// <param name="Status">Heartbeat status (e.g. <see cref="ClusterRpcStatus.StaleBrokerEpoch"/>).</param>
/// <param name="IsFenced">True if the broker is currently fenced.</param>
/// <param name="IsCaughtUp">True if the broker has caught up with the latest metadata.</param>
/// <param name="ShouldShutDown">True if the broker should proceed with shutdown.</param>
public sealed record BrokerHeartbeatOutcome(
    ClusterRpcStatus Status,
    bool IsFenced,
    bool IsCaughtUp,
    bool ShouldShutDown);

using Kuestenlogik.Surgewave.Clustering.Replication;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Neutral outcome of a broker registration RPC (#59 b5). Exposes only the fields the
/// caller reads: the controller-assigned epoch and a protocol-neutral status.
/// </summary>
/// <param name="Status">Registration status (<see cref="ClusterRpcStatus.None"/> on success).</param>
/// <param name="BrokerEpoch">The assigned broker epoch, or -1 if none was assigned.</param>
public sealed record BrokerRegistrationOutcome(ClusterRpcStatus Status, long BrokerEpoch);

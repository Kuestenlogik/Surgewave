using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc4 — the neutral, in-Clustering implementation of <see cref="INativeInterBrokerService"/>.
/// Applies decoded native inter-broker requests directly to <see cref="ClusterState"/>, matching the
/// partition-state application performed by the Kafka-wire <c>InterBrokerApiHandler</c> — without any
/// Protocol.Kafka dependency.
/// </summary>
/// <remarks>
/// <b>Inc5 prerequisite — controller-epoch fencing.</b> Unlike the Kafka-wire UpdateMetadata handler
/// (<c>InterBrokerApiHandler.HandleUpdateMetadata</c>), this applier does NOT reject stale controller
/// epochs or advance ControllerId/ControllerEpoch, because the Inc2 <see cref="PartitionStatesPayload"/>
/// is deliberately fire-and-forget and carries no controller epoch or live-broker list. That is safe
/// today because nothing SENDS native UpdateMetadata yet (the native controller client lands in Inc5).
/// Before a native sender is wired, the payload MUST be enriched with ControllerId/ControllerEpoch and
/// this applier MUST gate on <c>epoch &lt; ClusterState.ControllerEpoch</c> (returning
/// <see cref="ClusterRpcStatus.StaleControllerEpoch"/>) so a delayed push from a demoted controller
/// cannot regress partition metadata during failover.
/// </remarks>
public sealed class ClusterStateInterBrokerService(ClusterState clusterState) : INativeInterBrokerService
{
    private readonly ClusterState _clusterState = clusterState;

    public ValueTask<ClusterRpcStatus> ApplyUpdateMetadataAsync(PartitionStatesPayload payload, CancellationToken ct = default)
    {
        foreach (var (tp, state) in payload.Entries)
        {
            // Apply only the topology fields the controller owns (leader/epoch/replicas/ISR); local
            // watermarks/log offsets stay follower-owned, exactly as the Kafka-wire UpdateMetadata does.
            _clusterState.UpdatePartitionState(tp, s =>
            {
                s.LeaderBrokerId = state.LeaderBrokerId;
                s.LeaderEpoch = state.LeaderEpoch;
                s.Replicas.Clear();
                s.Replicas.AddRange(state.Replicas);
                s.Isr.Clear();
                s.Isr.AddRange(state.Isr);
            });
        }

        return ValueTask.FromResult(ClusterRpcStatus.None);
    }
}

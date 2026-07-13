using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc4/Inc5 — neutral server-side surface for native inter-broker control-plane RPCs. The native
/// receive server (<see cref="NativeInterBrokerServer"/>) decodes a frame and routes it here; the
/// concrete implementation applies the effect to local broker/cluster state. Kept protocol-neutral
/// (no Kafka DTOs) so it lives in <c>Clustering</c> and never pulls a Protocol.Kafka dependency.
/// <para>
/// Only the ops implemented so far appear here; later increments (native registration/heartbeat,
/// native txn markers) extend this surface as they wire their receive paths.
/// </para>
/// </summary>
public interface INativeInterBrokerService
{
    /// <summary>
    /// Apply the partition states carried by a controller UpdateMetadata push to local cluster state,
    /// returning the outcome status. Mirrors the partition-state application of the Kafka-wire
    /// UpdateMetadata handler, including controller-epoch fencing (a stale push from a demoted
    /// controller returns <see cref="ClusterRpcStatus.StaleControllerEpoch"/> and applies nothing).
    /// </summary>
    ValueTask<ClusterRpcStatus> ApplyUpdateMetadataAsync(PartitionStatesPayload payload, CancellationToken ct = default);

    /// <summary>
    /// Apply a controller LeaderAndIsr push: update local cluster state and turn each entry into a
    /// BecomeLeader/BecomeFollower transition on the replica manager, exactly like the Kafka-wire
    /// LeaderAndIsr handler. Fenced on the controller epoch like
    /// <see cref="ApplyUpdateMetadataAsync"/>.
    /// </summary>
    ValueTask<ClusterRpcStatus> ApplyLeaderAndIsrAsync(PartitionStatesPayload payload, CancellationToken ct = default);

    /// <summary>
    /// Apply a controller StopReplica push: stop replication for each partition and, when the delete
    /// flag is set, remove the partition's log and state. Fenced on the controller epoch, and refused
    /// (<see cref="ClusterRpcStatus.ReplicaNotAvailable"/>) when the payload targets another broker.
    /// </summary>
    ValueTask<ClusterRpcStatus> ApplyStopReplicaAsync(StopReplicaPayload payload, CancellationToken ct = default);

    /// <summary>
    /// Apply a leader's reverse ISR report (#69) via the controller-side ISR applier. Returns
    /// <see cref="ClusterRpcStatus.NotController"/> when this broker is not the controller.
    /// </summary>
    ValueTask<ClusterRpcStatus> ApplyIsrChangeAsync(AlterPartitionPayload payload, CancellationToken ct = default);

    /// <summary>
    /// Register a broker with the cluster-membership authority (#60 Inc6b) — the native counterpart of
    /// the Kafka-wire BrokerRegistration. Returns <see cref="ClusterRpcStatus.NotController"/> when this
    /// broker is not the controller, so the caller retries against the real controller.
    /// </summary>
    ValueTask<BrokerRegistrationOutcome> RegisterBrokerAsync(BrokerRegistrationInput input, CancellationToken ct = default);

    /// <summary>
    /// Process a broker heartbeat (#60 Inc6b). Returns <see cref="ClusterRpcStatus.NotController"/> when
    /// this broker is not the controller.
    /// </summary>
    ValueTask<BrokerHeartbeatOutcome> HeartbeatAsync(BrokerHeartbeatInput input, CancellationToken ct = default);
}

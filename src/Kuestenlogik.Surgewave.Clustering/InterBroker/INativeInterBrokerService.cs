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

    /// <summary>
    /// Apply a replicated transaction commit/abort marker to the leader's partition logs (#60 Inc7):
    /// append a control batch for each partition this broker leads and record it in the transaction
    /// index. Returns <see cref="ClusterRpcStatus.NotLeaderForPartition"/> if this broker does not lead
    /// a targeted partition (mirrors the Kafka-wire WriteTxnMarkers handler).
    /// </summary>
    ValueTask<ClusterRpcStatus> ApplyWriteTxnMarkersAsync(WriteTxnMarkersRequestPayload payload, CancellationToken ct = default);

    /// <summary>
    /// Take a departing broker's partition leaderships away (#180). Returns the partitions it
    /// still leads — empty when everything moved — and <see cref="ClusterRpcStatus.NotController"/>
    /// when this broker is not the controller, so the caller retries against the real one.
    /// </summary>
    ValueTask<ControlledShutdownResponsePayload> ApplyControlledShutdownAsync(
        ControlledShutdownPayload payload, CancellationToken ct = default);
}

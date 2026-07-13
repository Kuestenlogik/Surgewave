using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc4 — neutral server-side surface for native inter-broker control-plane RPCs. The native
/// receive server (<see cref="NativeInterBrokerServer"/>) decodes a frame and routes it here; the
/// concrete implementation applies the effect to local broker/cluster state. Kept protocol-neutral
/// (no Kafka DTOs) so it lives in <c>Clustering</c> and never pulls a Protocol.Kafka dependency.
/// <para>
/// Only the ops implemented so far appear here; later increments (native controller client, native
/// registration/heartbeat, native txn markers) extend this surface as they wire their receive paths.
/// </para>
/// </summary>
public interface INativeInterBrokerService
{
    /// <summary>
    /// Apply the partition states carried by a controller UpdateMetadata push to local cluster state,
    /// returning the outcome status. Mirrors the partition-state application of the Kafka-wire
    /// UpdateMetadata handler.
    /// </summary>
    ValueTask<ClusterRpcStatus> ApplyUpdateMetadataAsync(PartitionStatesPayload payload, CancellationToken ct = default);
}

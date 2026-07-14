namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// #72 Inc1 — the wire a controller push arrived on. Passed into
/// <see cref="ClusterState.TryAdvanceControllerEpoch(int,int,ControllerPushWire?)"/> so the epoch
/// fence and the Kafka-wire cap bookkeeping form ONE atomic operation under the state lock — a
/// separate note-after-fence would let a concurrent election's cap reset interleave between the two
/// and leave a freshly promoted controller capped for its whole reign.
/// </summary>
public enum ControllerPushWire : byte
{
    /// <summary>The push arrived over the Kafka compatibility wire (client port).</summary>
    KafkaWire = 0,

    /// <summary>The push arrived as a native SRWV frame (replication port).</summary>
    Native = 1,
}

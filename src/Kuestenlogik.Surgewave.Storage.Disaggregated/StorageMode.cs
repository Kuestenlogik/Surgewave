namespace Kuestenlogik.Surgewave.Storage.Disaggregated;

/// <summary>
/// Per-topic storage-mode selector introduced by ADR-014. Default is
/// <see cref="Replicated"/> (the existing local-segment + ISR path).
/// The two disaggregated modes opt the topic into an object-store-
/// durable write path; the broker enforces <c>replication.factor=1</c>
/// for those — S3 is the durability layer, replicating again would
/// only burn money.
/// </summary>
public enum StorageMode
{
    /// <summary>Existing replicated path (local segments + ISR). Default.</summary>
    Replicated,

    /// <summary>
    /// AutoMQ-style: broker keeps a local WAL on EBS/NVMe so produce-ack
    /// stays sub-10 ms, then a background flusher packs sealed segments
    /// into S3 stream objects and appends them to the partition manifest.
    /// Embedded-friendly — the WAL works locally even without an object
    /// store configured.
    /// </summary>
    DisaggregatedWal,

    /// <summary>
    /// WarpStream-style: incoming batches buffer in RAM on a stateless
    /// agent, the agent PUTs to S3 on size/time threshold, then commits
    /// the offset range to the manifest. No WAL. Produce-P99 is
    /// dominated by the S3 PUT (~400-600 ms). Not supported in embedded
    /// mode — needs an object store reachable at startup.
    /// </summary>
    DisaggregatedStateless,
}

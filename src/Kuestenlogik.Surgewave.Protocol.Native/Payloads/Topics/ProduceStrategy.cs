namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Per-topic write-path hint that the broker hands back inside the
/// metadata payload (ADR-014 §"Wire-protocol behaviour"). Lets a
/// native client pick the cheapest produce path for the topic without
/// the operator having to configure clients separately — the topic's
/// <c>storage.mode</c> drives it on the broker side.
///
/// Wire encoding is a single byte; values are stable.
/// </summary>
public enum ProduceStrategy : byte
{
    /// <summary>
    /// Classical replicated path: producer talks to the leader,
    /// leader replicates to ISR, ack returns after the quorum has the
    /// batch. Default for topics without <c>storage.mode</c> set.
    /// </summary>
    Replicated = 0,

    /// <summary>
    /// <c>storage.mode=disaggregated-wal</c>. From the client's point
    /// of view it is identical to <see cref="Replicated"/> — the
    /// broker handles the WAL + S3-offload internally. The hint is
    /// still surfaced so clients can show the operator that the
    /// topic is in disaggregated mode (cost-reporting, dashboards).
    /// </summary>
    WalViaBroker = 1,

    /// <summary>
    /// <c>storage.mode=disaggregated-stateless</c> with the broker as
    /// relay agent. The client sends Produce requests the usual way;
    /// the broker buffers + flushes to S3 + commits to manifest. Acks
    /// return after the S3 PUT (~400-600 ms tail-latency).
    /// </summary>
    StatelessViaBroker = 2,

    /// <summary>
    /// <c>storage.mode=disaggregated-stateless</c> with the client
    /// uploading directly to the object store via a presigned URL
    /// handed out by the broker. Skips the broker-relay round-trip.
    /// <strong>Reserved for a later optimisation pass</strong> — v1
    /// brokers never advertise this strategy; v1 clients must treat
    /// it as if it were <see cref="StatelessViaBroker"/> when they
    /// encounter it on a future broker.
    /// </summary>
    StatelessDirect = 3,
}

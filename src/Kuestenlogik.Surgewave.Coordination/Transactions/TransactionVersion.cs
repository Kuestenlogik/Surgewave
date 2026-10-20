namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// The transaction protocol a client speaks, read off the version of its request (Kafka's
/// <c>transaction.version</c> feature). With <see cref="Tv2"/> (KIP-890 part 2) the coordinator bumps the
/// producer epoch on every EndTxn and returns it, and the client no longer sends AddPartitionsToTxn or
/// AddOffsetsToTxn: produce v12+ and TxnOffsetCommit v5+ add the partition or group themselves.
/// </summary>
public enum TransactionVersion : short
{
    /// <summary>The original protocol: the client registers partitions and groups, the epoch stays per session.</summary>
    Tv0 = 0,

    /// <summary>Server-side verification of partitions that the client registered (KIP-890 part 1).</summary>
    Tv1 = 1,

    /// <summary>Epoch bump per transaction and implicit partition/group registration (KIP-890 part 2).</summary>
    Tv2 = 2,
}

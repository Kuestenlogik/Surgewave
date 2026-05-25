namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// Configuration for exactly-once source connector semantics.
/// Controls offset tracking behavior, batching, and transaction timeouts.
/// </summary>
public sealed class ExactlyOnceConfig
{
    /// <summary>
    /// Whether exactly-once source offset tracking is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// The compacted topic used to store source connector offsets.
    /// Offsets are keyed by connectorName:sourcePartition.
    /// </summary>
    public string OffsetTopic { get; set; } = "__connect_offsets";

    /// <summary>
    /// Maximum number of records to produce in a single transaction batch.
    /// Larger batches improve throughput but increase latency and memory usage.
    /// </summary>
    public int MaxBatchSize { get; set; } = 1000;

    /// <summary>
    /// Maximum time allowed for a cross-topic transaction to complete.
    /// Transactions exceeding this timeout will be aborted.
    /// </summary>
    public TimeSpan TransactionTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Interval between poll cycles when no records are available.
    /// Controls the idle-wait period before the next PollWithOffsetAsync call.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);
}

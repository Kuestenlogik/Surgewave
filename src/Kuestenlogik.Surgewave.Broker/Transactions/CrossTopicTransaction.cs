namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Represents a cross-topic transaction that buffers writes across multiple topics
/// and commits them atomically using two-phase commit.
/// </summary>
public sealed class CrossTopicTransaction
{
    private readonly Lock _lock = new();

    /// <summary>Unique transaction identifier.</summary>
    public string TransactionId { get; init; } = Guid.NewGuid().ToString("N");

    /// <summary>Current state of the transaction.</summary>
    public CrossTopicTransactionState State { get; set; } = CrossTopicTransactionState.Open;

    /// <summary>Buffered writes pending commit.</summary>
    public List<PendingWrite> PendingWrites { get; } = [];

    /// <summary>When the transaction was started.</summary>
    public DateTimeOffset StartedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>Transaction timeout. Auto-aborted if exceeded.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Optional producer identifier for tracking.</summary>
    public string? ProducerId { get; init; }

    /// <summary>Whether this transaction has exceeded its timeout.</summary>
    public bool IsTimedOut => State == CrossTopicTransactionState.Open &&
        DateTimeOffset.UtcNow - StartedAt > Timeout;

    /// <summary>
    /// Add a write to the transaction buffer. Thread-safe.
    /// </summary>
    public void AddWrite(string topic, int partition, byte[]? key, byte[] value, Dictionary<string, string>? headers = null)
    {
        lock (_lock)
        {
            if (State != CrossTopicTransactionState.Open)
                throw new InvalidOperationException($"Cannot add writes to transaction in state {State}");

            PendingWrites.Add(new PendingWrite
            {
                Topic = topic,
                Partition = partition,
                Key = key,
                Value = value,
                Headers = headers
            });
        }
    }

    /// <summary>
    /// Get distinct topics referenced by pending writes.
    /// </summary>
    public IReadOnlySet<string> GetTopics()
    {
        lock (_lock)
        {
            var topics = new HashSet<string>(StringComparer.Ordinal);
            foreach (var write in PendingWrites)
                topics.Add(write.Topic);
            return topics;
        }
    }
}

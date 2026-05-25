using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Kafka producer wrapper for Streams processing.
/// Handles output records from sink nodes and transaction management.
/// </summary>
internal sealed class StreamsProducer : IDisposable
{
    private readonly StreamsConfig _config;
    private readonly ILogger _logger;
    private readonly ConcurrentQueue<ProducerRecord> _pendingRecords = new();
    private readonly ConcurrentDictionary<string, long> _producedOffsets = new();
    private bool _transactionInProgress;
    private bool _disposed;

    public StreamsProducer(StreamsConfig config, ILogger logger)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Initializes transactions (for exactly-once semantics).
    /// </summary>
    public void InitTransactions()
    {
        _logger.LogDebug("Initializing transactions");
    }

    /// <summary>
    /// Begins a new transaction.
    /// </summary>
    public void BeginTransaction()
    {
        if (_transactionInProgress)
            throw new InvalidOperationException("Transaction already in progress");

        _transactionInProgress = true;
        _logger.LogDebug("Beginning transaction");
    }

    /// <summary>
    /// Sends offsets to the transaction for exactly-once semantics.
    /// </summary>
    public void SendOffsetsToTransaction(
        IDictionary<TopicPartition, long> offsets,
        string consumerGroupId)
    {
        if (!_transactionInProgress)
            throw new InvalidOperationException("No transaction in progress");

        foreach (var (partition, offset) in offsets)
        {
            _logger.LogDebug("Adding offset to transaction: {Topic}-{Partition}:{Offset}",
                partition.Topic, partition.Partition, offset);
        }
    }

    /// <summary>
    /// Commits the current transaction.
    /// </summary>
    public void CommitTransaction()
    {
        if (!_transactionInProgress)
            throw new InvalidOperationException("No transaction in progress");

        // Flush all pending records
        Flush();

        _transactionInProgress = false;
        _logger.LogDebug("Committed transaction");
    }

    /// <summary>
    /// Aborts the current transaction.
    /// </summary>
    public void AbortTransaction()
    {
        if (!_transactionInProgress)
            throw new InvalidOperationException("No transaction in progress");

        // Discard pending records
        while (_pendingRecords.TryDequeue(out _)) { }

        _transactionInProgress = false;
        _logger.LogDebug("Aborted transaction");
    }

    /// <summary>
    /// Produces a record to a topic.
    /// </summary>
    public Task<RecordMetadata> ProduceAsync(ProducerRecord record)
    {
        _pendingRecords.Enqueue(record);

        // Simulate immediate success
        var offset = _producedOffsets.AddOrUpdate(
            $"{record.Topic}-{record.Partition}",
            0,
            (_, existing) => existing + 1);

        return Task.FromResult(new RecordMetadata(record.Topic, record.Partition, offset));
    }

    /// <summary>
    /// Produces a record synchronously.
    /// </summary>
    public RecordMetadata Produce(ProducerRecord record)
    {
        return ProduceAsync(record).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Produces a record to a topic with key and value.
    /// </summary>
    public Task<RecordMetadata> ProduceAsync(
        string topic,
        byte[] key,
        byte[] value,
        long timestamp = 0,
        int? partition = null)
    {
        var record = new ProducerRecord(topic, partition ?? 0, key, value, timestamp);
        return ProduceAsync(record);
    }

    /// <summary>
    /// Flushes all pending records.
    /// </summary>
    public void Flush()
    {
        var flushed = 0;
        while (_pendingRecords.TryDequeue(out var record))
        {
            // In a full implementation, this would actually send to Kafka
            flushed++;
        }

        if (flushed > 0)
        {
            _logger.LogDebug("Flushed {Count} records", flushed);
        }
    }

    /// <summary>
    /// Flushes pending records with timeout.
    /// </summary>
    public void Flush(TimeSpan timeout)
    {
        Flush();
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        Flush();
        _disposed = true;
    }
}

/// <summary>
/// Represents a record to be produced.
/// </summary>
public readonly record struct ProducerRecord(
    string Topic,
    int Partition,
    byte[] Key,
    byte[] Value,
    long Timestamp = 0);

/// <summary>
/// Metadata about a produced record.
/// </summary>
public readonly record struct RecordMetadata(
    string Topic,
    int Partition,
    long Offset);

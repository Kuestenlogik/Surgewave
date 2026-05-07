using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Changelog;

/// <summary>
/// Writes state store changes to a changelog topic for durability and recovery.
/// </summary>
internal sealed class ChangelogWriter<TKey, TValue> : IDisposable
    where TKey : notnull
{
    private readonly string _topicName;
    private readonly int _partition;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly StreamsProducer? _producer;
    private readonly ILogger _logger;
    private readonly Queue<ChangelogRecord> _pendingRecords = new();
    private readonly int _maxBatchSize;
    private bool _disposed;

    public string TopicName => _topicName;
    public int Partition => _partition;

    public ChangelogWriter(
        string applicationId,
        string storeName,
        int partition,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        StreamsProducer? producer,
        ILogger logger,
        int maxBatchSize = 1000)
    {
        _topicName = $"{applicationId}-{storeName}-changelog";
        _partition = partition;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _producer = producer;
        _logger = logger;
        _maxBatchSize = maxBatchSize;
    }

    /// <summary>
    /// Writes a put operation to the changelog.
    /// </summary>
    public void Write(TKey key, TValue value, long timestamp)
    {
        var keyBytes = _keySerde.Serialize(key);
        var valueBytes = _valueSerde.Serialize(value);

        var record = new ChangelogRecord(keyBytes, valueBytes, timestamp);
        _pendingRecords.Enqueue(record);

        if (_pendingRecords.Count >= _maxBatchSize)
        {
            Flush();
        }
    }

    /// <summary>
    /// Writes a delete (tombstone) operation to the changelog.
    /// </summary>
    public void Delete(TKey key, long timestamp)
    {
        var keyBytes = _keySerde.Serialize(key);

        var record = new ChangelogRecord(keyBytes, [], timestamp);
        _pendingRecords.Enqueue(record);

        if (_pendingRecords.Count >= _maxBatchSize)
        {
            Flush();
        }
    }

    /// <summary>
    /// Flushes all pending records to the changelog topic.
    /// </summary>
    public void Flush()
    {
        if (_producer == null || _pendingRecords.Count == 0)
            return;

        var count = _pendingRecords.Count;

        while (_pendingRecords.TryDequeue(out var record))
        {
            try
            {
                _producer.Produce(new ProducerRecord(
                    _topicName,
                    _partition,
                    record.Key,
                    record.Value,
                    record.Timestamp));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write changelog record to {Topic}", _topicName);
                throw;
            }
        }

        _producer.Flush();
        _logger.LogDebug("Flushed {Count} records to changelog {Topic}", count, _topicName);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        Flush();
        _disposed = true;
    }

    private readonly record struct ChangelogRecord(byte[] Key, byte[] Value, long Timestamp);
}

/// <summary>
/// Reads state store changes from a changelog topic for recovery.
/// </summary>
internal sealed class ChangelogReader<TKey, TValue>
    where TKey : notnull
{
    private readonly string _topicName;
    private readonly int _partition;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly StreamsConsumer? _consumer;
    private readonly ILogger _logger;

    public string TopicName => _topicName;
    public int Partition => _partition;

    public ChangelogReader(
        string applicationId,
        string storeName,
        int partition,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        StreamsConsumer? consumer,
        ILogger logger)
    {
        _topicName = $"{applicationId}-{storeName}-changelog";
        _partition = partition;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _consumer = consumer;
        _logger = logger;
    }

    /// <summary>
    /// Restores the state store from the changelog topic.
    /// </summary>
    public async Task RestoreAsync(
        IKeyValueStore<TKey, TValue> store,
        CancellationToken cancellationToken)
    {
        if (_consumer == null)
        {
            _logger.LogDebug("No consumer available for changelog restoration");
            return;
        }

        _logger.LogInformation("Starting changelog restoration from {Topic}", _topicName);

        var partition = new TopicPartition(_topicName, _partition);
        var committedOffset = _consumer.Committed(partition) ?? 0;

        _consumer.Seek(partition, 0); // Start from beginning

        var restoredCount = 0L;
        var endOffset = long.MaxValue; // Would be fetched from broker in real implementation

        while (!cancellationToken.IsCancellationRequested)
        {
            var records = await _consumer.PollAsync(TimeSpan.FromMilliseconds(100), cancellationToken);

            if (records.Count == 0)
                break; // No more records

            foreach (var record in records)
            {
                if (record.Topic != _topicName || record.Partition != _partition)
                    continue;

                var key = _keySerde.Deserialize(record.Key);

                if (record.Value.Length == 0)
                {
                    // Tombstone - delete from store
                    store.Delete(key);
                }
                else
                {
                    var value = _valueSerde.Deserialize(record.Value);
                    store.Put(key, value);
                }

                restoredCount++;

                if (record.Offset >= endOffset)
                    break;
            }
        }

        // Seek back to committed offset for normal processing
        _consumer.Seek(partition, committedOffset);

        _logger.LogInformation("Restored {Count} records from changelog {Topic}", restoredCount, _topicName);
    }
}

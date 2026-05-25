using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Kafka consumer wrapper for Streams processing.
/// Manages subscription, polling, and offset tracking for stream tasks.
/// </summary>
internal sealed class StreamsConsumer : IDisposable
{
    private readonly StreamsConfig _config;
    private readonly ILogger _logger;
    private readonly List<string> _subscribedTopics = [];
    private readonly ConcurrentDictionary<TopicPartition, long> _currentOffsets = new();
    private readonly ConcurrentDictionary<TopicPartition, long> _committedOffsets = new();
    private readonly ConcurrentDictionary<TopicPartition, long> _highWatermarks = new();
    private bool _disposed;

    public event Action<IEnumerable<TopicPartition>>? PartitionsAssigned;
    public event Action<IEnumerable<TopicPartition>>? PartitionsRevoked;

    public IReadOnlyList<TopicPartition> Assignment => _currentOffsets.Keys.ToList();

    public StreamsConsumer(StreamsConfig config, ILogger logger)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Subscribes to a list of topics.
    /// </summary>
    public void Subscribe(IEnumerable<string> topics)
    {
        _subscribedTopics.Clear();
        _subscribedTopics.AddRange(topics);
        _logger.LogInformation("Subscribed to topics: {Topics}", string.Join(", ", _subscribedTopics));
    }

    /// <summary>
    /// Polls for new records.
    /// </summary>
    public async Task<IReadOnlyList<ConsumerRecord>> PollAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        // In a full implementation, this would call the Surgewave native client
        // For now, return empty to allow the framework to function
        await Task.Delay(timeout, cancellationToken).ConfigureAwait(false);
        return [];
    }

    /// <summary>
    /// Seeks to a specific offset.
    /// </summary>
    public void Seek(TopicPartition partition, long offset)
    {
        _currentOffsets[partition] = offset;
        _logger.LogDebug("Seeking {Topic}-{Partition} to offset {Offset}",
            partition.Topic, partition.Partition, offset);
    }

    /// <summary>
    /// Gets the current position for a partition.
    /// </summary>
    public long Position(TopicPartition partition)
    {
        return _currentOffsets.GetValueOrDefault(partition, 0);
    }

    /// <summary>
    /// Commits offsets synchronously.
    /// </summary>
    public void CommitSync()
    {
        foreach (var (partition, offset) in _currentOffsets)
        {
            _committedOffsets[partition] = offset;
        }
        _logger.LogDebug("Committed offsets for {Count} partitions", _currentOffsets.Count);
    }

    /// <summary>
    /// Commits offsets asynchronously.
    /// </summary>
    public Task CommitAsync()
    {
        CommitSync();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Commits specific offsets.
    /// </summary>
    public void Commit(IDictionary<TopicPartition, long> offsets)
    {
        foreach (var (partition, offset) in offsets)
        {
            _committedOffsets[partition] = offset;
            _currentOffsets[partition] = offset;
        }
    }

    /// <summary>
    /// Gets committed offset for a partition.
    /// </summary>
    public long? Committed(TopicPartition partition)
    {
        return _committedOffsets.TryGetValue(partition, out var offset) ? offset : null;
    }

    /// <summary>
    /// Gets the high watermark for a partition.
    /// </summary>
    public long GetHighWatermark(TopicPartition partition)
    {
        return _highWatermarks.GetValueOrDefault(partition, 0);
    }

    /// <summary>
    /// Updates the high watermark for a partition.
    /// </summary>
    public void UpdateHighWatermark(TopicPartition partition, long highWatermark)
    {
        _highWatermarks[partition] = highWatermark;
    }

    /// <summary>
    /// Gets the end (high watermark) offset for a topic-partition.
    /// Returns 0 if not available.
    /// </summary>
    public long GetEndOffset(TopicPartition tp)
    {
        return _highWatermarks.GetValueOrDefault(tp, 0);
    }

    /// <summary>
    /// Pauses consumption from specified partitions.
    /// </summary>
    public void Pause(IEnumerable<TopicPartition> partitions)
    {
        _logger.LogDebug("Pausing partitions: {Partitions}",
            string.Join(", ", partitions.Select(p => $"{p.Topic}-{p.Partition}")));
    }

    /// <summary>
    /// Resumes consumption from specified partitions.
    /// </summary>
    public void Resume(IEnumerable<TopicPartition> partitions)
    {
        _logger.LogDebug("Resuming partitions: {Partitions}",
            string.Join(", ", partitions.Select(p => $"{p.Topic}-{p.Partition}")));
    }

    /// <summary>
    /// Simulates a partition assignment (for testing/standalone mode).
    /// </summary>
    public void SimulateAssignment(IEnumerable<TopicPartition> partitions)
    {
        var partitionList = partitions.ToList();
        foreach (var partition in partitionList)
        {
            _currentOffsets[partition] = 0;
        }
        PartitionsAssigned?.Invoke(partitionList);
    }

    /// <summary>
    /// Simulates partition revocation (for testing/standalone mode).
    /// </summary>
    public void SimulateRevocation(IEnumerable<TopicPartition> partitions)
    {
        var partitionList = partitions.ToList();
        foreach (var partition in partitionList)
        {
            _currentOffsets.TryRemove(partition, out _);
        }
        PartitionsRevoked?.Invoke(partitionList);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _subscribedTopics.Clear();
        _currentOffsets.Clear();
        _committedOffsets.Clear();
        _highWatermarks.Clear();
        _disposed = true;
    }
}

/// <summary>
/// Represents a consumed record.
/// </summary>
public readonly record struct ConsumerRecord(
    string Topic,
    int Partition,
    long Offset,
    long Timestamp,
    byte[] Key,
    byte[] Value);

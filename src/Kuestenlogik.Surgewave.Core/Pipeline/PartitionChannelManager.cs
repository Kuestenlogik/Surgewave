using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Core.Pipeline;

/// <summary>
/// Manages per-partition write channels for zero-contention parallel writes.
/// Each partition has its own dedicated channel and worker thread.
/// </summary>
public sealed class PartitionChannelManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<TopicPartition, PartitionWriteChannel> _channels = new();
    private readonly Func<TopicPartition, IPartitionLog> _logProvider;
    private readonly int _channelCapacity;
    private readonly int _batchSize;
    private bool _disposed;

    public PartitionChannelManager(
        Func<TopicPartition, IPartitionLog> logProvider,
        int channelCapacity = 1000,
        int batchSize = 100)
    {
        _logProvider = logProvider;
        _channelCapacity = channelCapacity;
        _batchSize = batchSize;
    }

    /// <summary>
    /// Get or create a write channel for the specified partition.
    /// Each partition has exactly one channel for zero-contention writes.
    /// </summary>
    public PartitionWriteChannel GetOrCreateChannel(TopicPartition topicPartition)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return _channels.GetOrAdd(topicPartition, tp =>
            new PartitionWriteChannel(tp, _logProvider, _channelCapacity, _batchSize));
    }

    /// <summary>
    /// Write to a partition using its dedicated channel.
    /// Zero-contention with other partitions.
    /// </summary>
    public ValueTask<long> WriteAsync(TopicPartition topicPartition, ReadOnlyMemory<byte> recordBatch, CancellationToken cancellationToken = default)
    {
        var channel = GetOrCreateChannel(topicPartition);
        return channel.WriteAsync(recordBatch, cancellationToken);
    }

    /// <summary>
    /// Remove a partition's channel (e.g., when partition is deleted).
    /// </summary>
    public async ValueTask RemoveChannelAsync(TopicPartition topicPartition)
    {
        if (_channels.TryRemove(topicPartition, out var channel))
        {
            await channel.DisposeAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        var disposeTasks = _channels.Values.Select(c => c.DisposeAsync().AsTask());
        await Task.WhenAll(disposeTasks);
        _channels.Clear();
    }
}

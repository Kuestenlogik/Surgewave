using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Streams.Runtime;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Maintains the metadata view of all known Streams application instances.
/// Thread-safe for concurrent metadata updates and queries.
/// </summary>
public sealed class StreamsMetadataState
{
    private readonly ConcurrentDictionary<HostInfo, StreamsMetadata> _instances = new();
    private readonly HostInfo _localHost;

    public StreamsMetadataState(HostInfo localHost)
    {
        _localHost = localHost;
    }

    /// <summary>
    /// All known instance metadata.
    /// </summary>
    public IReadOnlyCollection<StreamsMetadata> All => _instances.Values.ToList();

    /// <summary>
    /// Update metadata for a specific instance (self or peer).
    /// </summary>
    public void UpdateMetadata(StreamsMetadata metadata)
    {
        _instances[metadata.HostInfo] = metadata;
    }

    /// <summary>
    /// Remove a peer from the metadata state.
    /// </summary>
    public bool RemovePeer(HostInfo host) => _instances.TryRemove(host, out _);

    /// <summary>
    /// Get metadata for all instances that have a specific store.
    /// </summary>
    public IReadOnlyCollection<StreamsMetadata> ForStore(string storeName)
    {
        return _instances.Values
            .Where(m => m.HasStateStore(storeName))
            .ToList();
    }

    /// <summary>
    /// Find the instance owning a specific partition and store.
    /// </summary>
    public StreamsMetadata? FindByPartitionAndStore(int partition, string storeName)
    {
        return _instances.Values.FirstOrDefault(m =>
            m.HasStateStore(storeName) && m.HasPartitionNumber(partition));
    }

    /// <summary>
    /// Get the total number of partitions across all instances for any topic.
    /// </summary>
    public int GetMaxPartitionCount()
    {
        var maxPartition = -1;
        foreach (var meta in _instances.Values)
        {
            foreach (var tp in meta.TopicPartitions)
            {
                if (tp.Partition > maxPartition)
                    maxPartition = tp.Partition;
            }
        }
        return maxPartition + 1;
    }

    /// <summary>
    /// Check if a host is the local instance.
    /// </summary>
    public bool IsLocal(HostInfo host) => host == _localHost;

    /// <summary>
    /// Check if a partition is owned by the local instance.
    /// </summary>
    public bool IsLocalPartition(int partition)
    {
        if (_instances.TryGetValue(_localHost, out var local))
            return local.HasPartitionNumber(partition);
        return false;
    }
}

using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Tracks consumer rack affinity for partitions.
/// Used by LeaderLocalityStrategy to optimize leader placement.
/// </summary>
public sealed class ConsumerRackTracker
{
    private readonly ConcurrentDictionary<TopicPartition, RackConsumerInfo> _partitionConsumers = new();
    private readonly ConcurrentDictionary<string, string> _consumerRacks = new();
    private readonly TimeSpan _consumerTimeout;

    public ConsumerRackTracker(TimeSpan? consumerTimeout = null)
    {
        _consumerTimeout = consumerTimeout ?? TimeSpan.FromMinutes(5);
    }

    /// <summary>
    /// Registers a consumer with its rack.
    /// </summary>
    /// <param name="consumerId">The consumer's member ID.</param>
    /// <param name="rack">The consumer's rack (from client.rack config).</param>
    public void RegisterConsumer(string consumerId, string? rack)
    {
        if (string.IsNullOrEmpty(rack)) return;
        _consumerRacks[consumerId] = rack;
    }

    /// <summary>
    /// Unregisters a consumer.
    /// </summary>
    public void UnregisterConsumer(string consumerId)
    {
        _consumerRacks.TryRemove(consumerId, out _);

        // Remove consumer from partition tracking
        foreach (var kvp in _partitionConsumers)
        {
            kvp.Value.ConsumerLastSeen.TryRemove(consumerId, out _);
        }
    }

    /// <summary>
    /// Records a fetch from a consumer for a partition.
    /// </summary>
    /// <param name="partition">The partition being fetched.</param>
    /// <param name="consumerId">The consumer's member ID.</param>
    public void RecordFetch(TopicPartition partition, string consumerId)
    {
        if (!_consumerRacks.TryGetValue(consumerId, out var rack))
            return;

        var info = _partitionConsumers.GetOrAdd(partition, _ => new RackConsumerInfo());
        info.ConsumerLastSeen[consumerId] = DateTimeOffset.UtcNow;
        info.ConsumerRacks[consumerId] = rack;
    }

    /// <summary>
    /// Gets the dominant rack for a partition based on consumer activity.
    /// </summary>
    /// <param name="partition">The partition to analyze.</param>
    /// <returns>The rack with the most active consumers, or null if none.</returns>
    public string? GetDominantRack(TopicPartition partition)
    {
        if (!_partitionConsumers.TryGetValue(partition, out var info))
            return null;

        var now = DateTimeOffset.UtcNow;
        var cutoff = now - _consumerTimeout;

        // Count active consumers per rack
        var rackCounts = new Dictionary<string, int>();
        foreach (var kvp in info.ConsumerLastSeen)
        {
            if (kvp.Value >= cutoff && info.ConsumerRacks.TryGetValue(kvp.Key, out var rack))
            {
                rackCounts.TryGetValue(rack, out var count);
                rackCounts[rack] = count + 1;
            }
        }

        if (rackCounts.Count == 0)
            return null;

        // Return rack with most consumers
        return rackCounts.MaxBy(kvp => kvp.Value).Key;
    }

    /// <summary>
    /// Gets consumer counts by rack for a partition.
    /// </summary>
    public Dictionary<string, int> GetRackConsumerCounts(TopicPartition partition)
    {
        var result = new Dictionary<string, int>();

        if (!_partitionConsumers.TryGetValue(partition, out var info))
            return result;

        var now = DateTimeOffset.UtcNow;
        var cutoff = now - _consumerTimeout;

        foreach (var kvp in info.ConsumerLastSeen)
        {
            if (kvp.Value >= cutoff && info.ConsumerRacks.TryGetValue(kvp.Key, out var rack))
            {
                result.TryGetValue(rack, out var count);
                result[rack] = count + 1;
            }
        }

        return result;
    }

    /// <summary>
    /// Gets all racks with active consumers for a partition.
    /// </summary>
    public IEnumerable<string> GetActiveRacks(TopicPartition partition)
    {
        if (!_partitionConsumers.TryGetValue(partition, out var info))
            return [];

        var now = DateTimeOffset.UtcNow;
        var cutoff = now - _consumerTimeout;

        return info.ConsumerLastSeen
            .Where(kvp => kvp.Value >= cutoff)
            .Select(kvp => info.ConsumerRacks.GetValueOrDefault(kvp.Key))
            .Where(rack => !string.IsNullOrEmpty(rack))
            .Distinct()!;
    }

    /// <summary>
    /// Cleans up stale consumer entries.
    /// </summary>
    public void Cleanup()
    {
        var cutoff = DateTimeOffset.UtcNow - _consumerTimeout;

        foreach (var kvp in _partitionConsumers)
        {
            var staleConsumers = kvp.Value.ConsumerLastSeen
                .Where(c => c.Value < cutoff)
                .Select(c => c.Key)
                .ToList();

            foreach (var consumer in staleConsumers)
            {
                kvp.Value.ConsumerLastSeen.TryRemove(consumer, out _);
                kvp.Value.ConsumerRacks.TryRemove(consumer, out _);
            }
        }
    }

    private sealed class RackConsumerInfo
    {
        public ConcurrentDictionary<string, DateTimeOffset> ConsumerLastSeen { get; } = new();
        public ConcurrentDictionary<string, string> ConsumerRacks { get; } = new();
    }
}

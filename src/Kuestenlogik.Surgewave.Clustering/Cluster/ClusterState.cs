using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Maintains the cluster-wide state including brokers, topics, and partition assignments.
/// Thread-safe for concurrent access.
/// </summary>
public sealed class ClusterState
{
    private readonly ConcurrentDictionary<int, BrokerNode> _brokers = new();
    private readonly ConcurrentDictionary<string, TopicMetadata> _topics = new();
    private readonly ConcurrentDictionary<TopicPartition, PartitionState> _partitionStates = new();
    private readonly object _stateLock = new();

    /// <summary>
    /// Current controller broker ID.
    /// </summary>
    public int ControllerId { get; set; } = -1;

    /// <summary>
    /// Controller epoch (increments on controller election).
    /// </summary>
    public int ControllerEpoch { get; set; }

    /// <summary>
    /// This broker's ID.
    /// </summary>
    public int LocalBrokerId { get; set; }

    /// <summary>
    /// Metadata version (increments on each metadata change).
    /// Used for tracking metadata consistency across brokers.
    /// </summary>
    public long MetadataVersion
    {
        get => Interlocked.Read(ref _metadataVersion);
        set => Interlocked.Exchange(ref _metadataVersion, value);
    }

    private long _metadataVersion;

    /// <summary>
    /// Atomically increment and return the new metadata version.
    /// </summary>
    public long IncrementMetadataVersion() => Interlocked.Increment(ref _metadataVersion);

    /// <summary>
    /// All known brokers in the cluster.
    /// </summary>
    public IReadOnlyDictionary<int, BrokerNode> Brokers => _brokers;

    /// <summary>
    /// All topics.
    /// </summary>
    public IReadOnlyDictionary<string, TopicMetadata> Topics => _topics;

    /// <summary>
    /// All partition states.
    /// </summary>
    public IReadOnlyDictionary<TopicPartition, PartitionState> PartitionStates => _partitionStates;

    public void AddBroker(BrokerNode broker)
    {
        _brokers[broker.BrokerId] = broker;
    }

    /// <summary>
    /// Register or update a broker with the given ID, host, and port.
    /// </summary>
    public void RegisterBroker(int brokerId, string host, int port, string? rack = null)
    {
        _brokers[brokerId] = new BrokerNode
        {
            BrokerId = brokerId,
            Host = host,
            Port = port,
            Rack = rack
        };
    }

    public void RemoveBroker(int brokerId)
    {
        _brokers.TryRemove(brokerId, out _);
    }

    public BrokerNode? GetBroker(int brokerId)
    {
        return _brokers.TryGetValue(brokerId, out var broker) ? broker : null;
    }

    public void AddTopic(TopicMetadata topic)
    {
        _topics[topic.Name] = topic;
    }

    public void RemoveTopic(string topicName)
    {
        _topics.TryRemove(topicName, out _);

        // Remove partition states
        var partitionsToRemove = _partitionStates.Keys
            .Where(tp => tp.Topic == topicName)
            .ToList();

        foreach (var tp in partitionsToRemove)
        {
            _partitionStates.TryRemove(tp, out _);
        }
    }

    public TopicMetadata? GetTopic(string topicName)
    {
        return _topics.TryGetValue(topicName, out var topic) ? topic : null;
    }

    public PartitionState? GetPartitionState(TopicPartition tp)
    {
        return _partitionStates.TryGetValue(tp, out var state) ? state : null;
    }

    public PartitionState GetOrCreatePartitionState(TopicPartition tp)
    {
        return _partitionStates.GetOrAdd(tp, _ => new PartitionState { TopicPartition = tp });
    }

    public void UpdatePartitionState(TopicPartition tp, Action<PartitionState> update)
    {
        lock (_stateLock)
        {
            var state = GetOrCreatePartitionState(tp);
            update(state);
        }
    }

    /// <summary>
    /// Remove partition state for a specific partition.
    /// </summary>
    public bool RemovePartitionState(TopicPartition tp)
    {
        return _partitionStates.TryRemove(tp, out _);
    }

    /// <summary>
    /// Get all partition states as key-value pairs.
    /// </summary>
    public IEnumerable<(TopicPartition, PartitionState)> GetAllPartitionStates()
    {
        return _partitionStates.Select(kvp => (kvp.Key, kvp.Value));
    }

    /// <summary>
    /// Get all partitions where this broker is the leader.
    /// </summary>
    public IEnumerable<TopicPartition> GetLeaderPartitions(int brokerId)
    {
        return _partitionStates
            .Where(kvp => kvp.Value.LeaderBrokerId == brokerId)
            .Select(kvp => kvp.Key);
    }

    /// <summary>
    /// Get all partitions where this broker is a follower.
    /// </summary>
    public IEnumerable<TopicPartition> GetFollowerPartitions(int brokerId)
    {
        return _partitionStates
            .Where(kvp => kvp.Value.Replicas.Contains(brokerId) && kvp.Value.LeaderBrokerId != brokerId)
            .Select(kvp => kvp.Key);
    }

    /// <summary>
    /// Get all partitions assigned to this broker (leader or follower).
    /// </summary>
    public IEnumerable<TopicPartition> GetAssignedPartitions(int brokerId)
    {
        return _partitionStates
            .Where(kvp => kvp.Value.Replicas.Contains(brokerId))
            .Select(kvp => kvp.Key);
    }

    /// <summary>
    /// Check if this broker is the leader for the given partition.
    /// </summary>
    public bool IsLeader(TopicPartition tp, int brokerId)
    {
        return _partitionStates.TryGetValue(tp, out var state) && state.LeaderBrokerId == brokerId;
    }

    /// <summary>
    /// Assign replicas to a partition.
    /// </summary>
    public void AssignReplicas(TopicPartition tp, List<int> replicas, int minIsr = 1)
    {
        lock (_stateLock)
        {
            var state = GetOrCreatePartitionState(tp);
            state.Replicas.Clear();
            state.Replicas.AddRange(replicas);
            state.MinInSyncReplicas = minIsr;

            // If no leader, elect from replicas
            if (state.LeaderBrokerId == -1 && replicas.Count > 0)
            {
                ElectLeader(tp, replicas[0]);
            }
        }
    }

    /// <summary>
    /// Elect a new leader for the partition.
    /// </summary>
    public bool ElectLeader(TopicPartition tp, int preferredLeader = -1)
    {
        lock (_stateLock)
        {
            var state = GetOrCreatePartitionState(tp);

            // Try preferred leader first
            if (preferredLeader >= 0 && state.Replicas.Contains(preferredLeader))
            {
                state.LeaderBrokerId = preferredLeader;
                state.LeaderEpoch++;

                // Add to ISR if not already
                if (!state.Isr.Contains(preferredLeader))
                {
                    state.Isr.Add(preferredLeader);
                }

                return true;
            }

            // Otherwise elect from ISR
            if (state.Isr.Count > 0)
            {
                state.LeaderBrokerId = state.Isr[0];
                state.LeaderEpoch++;
                return true;
            }

            // Unclean leader election: elect from any replica
            if (state.Replicas.Count > 0)
            {
                state.LeaderBrokerId = state.Replicas[0];
                state.LeaderEpoch++;
                state.Isr.Clear();
                state.Isr.Add(state.LeaderBrokerId);
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Update ISR for a partition.
    /// </summary>
    public void UpdateIsr(TopicPartition tp, List<int> isr)
    {
        lock (_stateLock)
        {
            var state = GetOrCreatePartitionState(tp);
            state.Isr.Clear();
            state.Isr.AddRange(isr);
        }
    }

    /// <summary>
    /// Add a replica to the ISR.
    /// </summary>
    /// <returns>True if the broker was actually added (wasn't already in ISR).</returns>
    public bool AddToIsr(TopicPartition tp, int brokerId)
    {
        lock (_stateLock)
        {
            var state = GetOrCreatePartitionState(tp);
            if (!state.Isr.Contains(brokerId))
            {
                state.Isr.Add(brokerId);
                return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Remove a replica from the ISR.
    /// </summary>
    /// <returns>True if the broker was actually removed (was in ISR).</returns>
    public bool RemoveFromIsr(TopicPartition tp, int brokerId)
    {
        lock (_stateLock)
        {
            var state = GetOrCreatePartitionState(tp);
            return state.Isr.Remove(brokerId);
        }
    }

    /// <summary>
    /// Clear all cluster state (for snapshot restoration).
    /// </summary>
    public void Clear()
    {
        lock (_stateLock)
        {
            _brokers.Clear();
            _topics.Clear();
            _partitionStates.Clear();
            ControllerId = -1;
            ControllerEpoch = 0;
        }
    }

    /// <summary>
    /// Set partition state directly (for snapshot restoration).
    /// </summary>
    public void SetPartitionState(TopicPartition tp, PartitionState state)
    {
        _partitionStates[tp] = state;
    }
}

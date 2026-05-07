using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Provides a unified interface for querying cluster metadata.
/// Abstracts over Raft and non-Raft modes.
/// </summary>
public sealed partial class MetadataService
{
    private readonly ILogger<MetadataService> _logger;
    private readonly ClusterState _clusterState;
    private readonly ClusteringConfig _config;
    private readonly RaftNode? _raftNode;

    private long _metadataVersion;

    public MetadataService(
        ILogger<MetadataService> logger,
        ClusterState clusterState,
        ClusteringConfig config,
        RaftNode? raftNode = null)
    {
        _logger = logger;
        _clusterState = clusterState;
        _config = config;
        _raftNode = raftNode;
    }

    /// <summary>
    /// Current metadata version (increments on any metadata change).
    /// In Raft mode, this is the Raft commit index.
    /// </summary>
    public long MetadataVersion => _config.UseRaftConsensus && _raftNode != null
        ? _raftNode.CommitIndex
        : _metadataVersion;

    /// <summary>
    /// Increment metadata version (for non-Raft mode).
    /// </summary>
    public void IncrementVersion()
    {
        if (!_config.UseRaftConsensus)
        {
            Interlocked.Increment(ref _metadataVersion);
        }
    }

    /// <summary>
    /// Get all brokers in the cluster.
    /// </summary>
    public IReadOnlyList<BrokerNode> GetBrokers()
    {
        return _clusterState.Brokers.Values.ToList();
    }

    /// <summary>
    /// Get a specific broker by ID.
    /// </summary>
    public BrokerNode? GetBroker(int brokerId)
    {
        return _clusterState.GetBroker(brokerId);
    }

    /// <summary>
    /// Get all topics.
    /// </summary>
    public IReadOnlyList<TopicMetadata> GetTopics()
    {
        return _clusterState.Topics.Values.ToList();
    }

    /// <summary>
    /// Get a specific topic by name.
    /// </summary>
    public TopicMetadata? GetTopic(string topicName)
    {
        return _clusterState.GetTopic(topicName);
    }

    /// <summary>
    /// Get partition state for a topic-partition.
    /// </summary>
    public PartitionState? GetPartitionState(TopicPartition tp)
    {
        return _clusterState.GetPartitionState(tp);
    }

    /// <summary>
    /// Get all partition states for a topic.
    /// </summary>
    public IReadOnlyList<PartitionState> GetPartitionStates(string topicName)
    {
        return _clusterState.PartitionStates
            .Where(kv => kv.Key.Topic == topicName)
            .Select(kv => kv.Value)
            .OrderBy(ps => ps.TopicPartition.Partition)
            .ToList();
    }

    /// <summary>
    /// Get the current controller broker ID.
    /// </summary>
    public int ControllerId => _clusterState.ControllerId;

    /// <summary>
    /// Get the current controller epoch.
    /// </summary>
    public int ControllerEpoch => _clusterState.ControllerEpoch;

    /// <summary>
    /// Check if a specific broker is the controller.
    /// </summary>
    public bool IsController(int brokerId)
    {
        return _clusterState.ControllerId == brokerId;
    }

    /// <summary>
    /// Get leader for a partition.
    /// </summary>
    public int GetLeader(TopicPartition tp)
    {
        var state = _clusterState.GetPartitionState(tp);
        return state?.LeaderBrokerId ?? -1;
    }

    /// <summary>
    /// Get ISR for a partition.
    /// </summary>
    public IReadOnlyList<int> GetIsr(TopicPartition tp)
    {
        var state = _clusterState.GetPartitionState(tp);
        return state?.Isr.ToList() ?? [];
    }

    /// <summary>
    /// Get replicas for a partition.
    /// </summary>
    public IReadOnlyList<int> GetReplicas(TopicPartition tp)
    {
        var state = _clusterState.GetPartitionState(tp);
        return state?.Replicas.ToList() ?? [];
    }

    /// <summary>
    /// Check if a broker is in ISR for a partition.
    /// </summary>
    public bool IsInIsr(TopicPartition tp, int brokerId)
    {
        var state = _clusterState.GetPartitionState(tp);
        return state?.Isr.Contains(brokerId) ?? false;
    }

    /// <summary>
    /// Check if Raft consensus is enabled and functioning.
    /// </summary>
    public bool IsRaftEnabled => _config.UseRaftConsensus && _raftNode != null;

    /// <summary>
    /// Get Raft state information (for diagnostics).
    /// </summary>
    public RaftStateInfo? GetRaftState()
    {
        if (_raftNode == null)
            return null;

        return new RaftStateInfo
        {
            NodeId = _raftNode.NodeId,
            State = _raftNode.State.ToString(),
            CurrentTerm = _raftNode.CurrentTerm,
            LeaderId = _raftNode.LeaderId,
            CommitIndex = _raftNode.CommitIndex,
            LastLogIndex = _raftNode.LastLogIndex,
            IsLeader = _raftNode.IsLeader
        };
    }

    /// <summary>
    /// Get a snapshot of the full cluster metadata.
    /// </summary>
    public ClusterMetadataSnapshot GetSnapshot()
    {
        return new ClusterMetadataSnapshot
        {
            Version = MetadataVersion,
            ControllerId = ControllerId,
            ControllerEpoch = ControllerEpoch,
            Brokers = GetBrokers().ToList(),
            Topics = GetTopics().ToList(),
            Partitions = _clusterState.PartitionStates
                .Select(kv => new PartitionSnapshot
                {
                    Topic = kv.Key.Topic,
                    Partition = kv.Key.Partition,
                    Leader = kv.Value.LeaderBrokerId,
                    LeaderEpoch = kv.Value.LeaderEpoch,
                    Replicas = kv.Value.Replicas.ToList(),
                    Isr = kv.Value.Isr.ToList()
                })
                .OrderBy(p => p.Topic)
                .ThenBy(p => p.Partition)
                .ToList(),
            RaftState = GetRaftState()
        };
    }
}

/// <summary>
/// Information about Raft node state.
/// </summary>
public sealed class RaftStateInfo
{
    public int NodeId { get; init; }
    public required string State { get; init; }
    public int CurrentTerm { get; init; }
    public int? LeaderId { get; init; }
    public long CommitIndex { get; init; }
    public long LastLogIndex { get; init; }
    public bool IsLeader { get; init; }
}

/// <summary>
/// Snapshot of cluster metadata.
/// </summary>
public sealed class ClusterMetadataSnapshot
{
    public long Version { get; init; }
    public int ControllerId { get; init; }
    public int ControllerEpoch { get; init; }
    public List<BrokerNode> Brokers { get; init; } = [];
    public List<TopicMetadata> Topics { get; init; } = [];
    public List<PartitionSnapshot> Partitions { get; init; } = [];
    public RaftStateInfo? RaftState { get; init; }
}

/// <summary>
/// Partition info in a metadata snapshot.
/// </summary>
public sealed class PartitionSnapshot
{
    public required string Topic { get; init; }
    public int Partition { get; init; }
    public int Leader { get; init; }
    public int LeaderEpoch { get; init; }
    public List<int> Replicas { get; init; } = [];
    public List<int> Isr { get; init; } = [];
}

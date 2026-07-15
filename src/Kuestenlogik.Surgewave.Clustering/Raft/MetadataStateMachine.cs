using System.Text.Json;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// State machine that applies committed Raft log entries to the cluster metadata.
/// Maintains the cluster state (topics, partitions, brokers, ISR).
/// </summary>
public sealed partial class MetadataStateMachine : IRaftStateMachine
{
    private readonly ILogger<MetadataStateMachine> _logger;
    private readonly ClusterState _clusterState;
    private readonly ClusterMembershipService _membership;

    public MetadataStateMachine(
        ILogger<MetadataStateMachine> logger,
        ClusterState clusterState,
        ClusterMembershipService membership)
    {
        _logger = logger;
        _clusterState = clusterState;
        _membership = membership;
    }

    public void Apply(RaftLogEntry entry)
    {
        try
        {
            switch (entry.CommandType)
            {
                case MetadataCommandType.Noop:
                    // No-op, used for leader confirmation
                    break;

                case MetadataCommandType.BrokerRegistered:
                    ApplyBrokerRegistered(entry);
                    break;

                case MetadataCommandType.BrokerRemoved:
                    ApplyBrokerRemoved(entry.Data);
                    break;

                case MetadataCommandType.TopicCreated:
                    ApplyTopicCreated(entry.Data);
                    break;

                case MetadataCommandType.TopicDeleted:
                    ApplyTopicDeleted(entry.Data);
                    break;

                case MetadataCommandType.PartitionAssigned:
                    ApplyPartitionAssigned(entry.Data);
                    break;

                case MetadataCommandType.IsrChanged:
                    ApplyIsrChanged(entry.Data);
                    break;

                case MetadataCommandType.LeaderChanged:
                    ApplyLeaderChanged(entry.Data);
                    break;

                case MetadataCommandType.ConfigChanged:
                    ApplyConfigChanged(entry.Data);
                    break;

                default:
                    LogUnknownCommandType(entry.CommandType, entry.Index);
                    break;
            }
        }
        catch (Exception ex)
        {
            LogApplyError(entry.CommandType, entry.Index, ex);
        }
    }

    public Task<byte[]> CreateSnapshotAsync(CancellationToken ct)
    {
        var snapshot = new MetadataSnapshot
        {
            Brokers = _clusterState.Brokers.Values.ToList(),
            Topics = _clusterState.Topics.Values.ToList(),
            PartitionStates = _clusterState.PartitionStates
                .Select(kv => new PartitionStateSnapshot
                {
                    Topic = kv.Key.Topic,
                    Partition = kv.Key.Partition,
                    State = kv.Value
                })
                .ToList(),
            ControllerId = _clusterState.ControllerId,
            ControllerEpoch = _clusterState.ControllerEpoch
        };

        var json = JsonSerializer.SerializeToUtf8Bytes(snapshot, ClusteringJsonContext.Default.MetadataSnapshot);
        return Task.FromResult(json);
    }

    public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct)
    {
        var data = JsonSerializer.Deserialize(snapshot, ClusteringJsonContext.Default.MetadataSnapshot);
        if (data == null)
            return Task.CompletedTask;

        // Clear existing state
        _clusterState.Clear();

        // Restore brokers
        foreach (var broker in data.Brokers)
        {
            _clusterState.AddBroker(broker);
        }

        // Restore topics
        foreach (var topic in data.Topics)
        {
            _clusterState.AddTopic(topic);
        }

        // Restore partition states
        foreach (var ps in data.PartitionStates)
        {
            var tp = new TopicPartition { Topic = ps.Topic, Partition = ps.Partition };
            _clusterState.SetPartitionState(tp, ps.State);
        }

        _clusterState.ControllerId = data.ControllerId;
        _clusterState.ControllerEpoch = data.ControllerEpoch;

        LogSnapshotRestored(data.Brokers.Count, data.Topics.Count, data.PartitionStates.Count);

        return Task.CompletedTask;
    }

    private void ApplyBrokerRegistered(RaftLogEntry entry)
    {
        var cmd = JsonSerializer.Deserialize(entry.Data, ClusteringJsonContext.Default.BrokerRegisteredCommand);
        if (cmd == null) return;

        // #72 Inc5 — in Raft mode the broker epoch is the committed log INDEX: durable, replicated and
        // strictly monotone across failover (KRaft parity), so no node-local composed mint is needed.
        // Rebuild the membership store on replay through the shared authority (UpdateBroker-merge, never
        // an AddBroker-clobber that would drop a discovered replication port). Idempotent on re-apply.
        _membership.ApplyReplicatedRegistration(
            cmd.BrokerId, cmd.IncarnationId, epoch: entry.Index, cmd.Host, cmd.Port, cmd.Rack,
            cmd.InterBrokerProtocol, cmd.ReplicationPort == 0 ? null : cmd.ReplicationPort);

        LogBrokerRegistered(cmd.BrokerId, cmd.Host, cmd.Port);
    }

    private void ApplyBrokerRemoved(byte[] data)
    {
        var cmd = JsonSerializer.Deserialize(data, ClusteringJsonContext.Default.BrokerRemovedCommand);
        if (cmd == null) return;

        _clusterState.RemoveBroker(cmd.BrokerId);
        LogBrokerRemoved(cmd.BrokerId);
    }

    private void ApplyTopicCreated(byte[] data)
    {
        var cmd = JsonSerializer.Deserialize(data, ClusteringJsonContext.Default.TopicCreatedCommand);
        if (cmd == null) return;

        var topic = new TopicMetadata
        {
            Name = cmd.Name,
            TopicId = cmd.TopicId,
            PartitionCount = cmd.PartitionCount,
            ReplicationFactor = cmd.ReplicationFactor,
            Config = cmd.Config ?? [],
            CreatedAt = DateTime.UtcNow
        };

        _clusterState.AddTopic(topic);
        LogTopicCreated(cmd.Name, cmd.PartitionCount, cmd.ReplicationFactor);
    }

    private void ApplyTopicDeleted(byte[] data)
    {
        var cmd = JsonSerializer.Deserialize(data, ClusteringJsonContext.Default.TopicDeletedCommand);
        if (cmd == null) return;

        _clusterState.RemoveTopic(cmd.Name);
        LogTopicDeleted(cmd.Name);
    }

    private void ApplyPartitionAssigned(byte[] data)
    {
        var cmd = JsonSerializer.Deserialize(data, ClusteringJsonContext.Default.PartitionAssignedCommand);
        if (cmd == null) return;

        var tp = new TopicPartition { Topic = cmd.Topic, Partition = cmd.Partition };
        _clusterState.AssignReplicas(tp, cmd.Replicas, cmd.MinIsr);
        LogPartitionAssigned(cmd.Topic, cmd.Partition, string.Join(",", cmd.Replicas));
    }

    private void ApplyIsrChanged(byte[] data)
    {
        var cmd = JsonSerializer.Deserialize(data, ClusteringJsonContext.Default.IsrChangedCommand);
        if (cmd == null) return;

        var tp = new TopicPartition { Topic = cmd.Topic, Partition = cmd.Partition };
        _clusterState.UpdateIsr(tp, cmd.Isr);
        LogIsrChanged(cmd.Topic, cmd.Partition, string.Join(",", cmd.Isr));
    }

    private void ApplyLeaderChanged(byte[] data)
    {
        var cmd = JsonSerializer.Deserialize(data, ClusteringJsonContext.Default.LeaderChangedCommand);
        if (cmd == null) return;

        var tp = new TopicPartition { Topic = cmd.Topic, Partition = cmd.Partition };
        _clusterState.ElectLeader(tp, cmd.NewLeader);
        LogLeaderChanged(cmd.Topic, cmd.Partition, cmd.NewLeader, cmd.LeaderEpoch);
    }

    private void ApplyConfigChanged(byte[] data)
    {
        var cmd = JsonSerializer.Deserialize(data, ClusteringJsonContext.Default.ConfigChangedCommand);
        if (cmd == null) return;

        if (_clusterState.Topics.TryGetValue(cmd.Topic, out var topic))
        {
            topic.Config[cmd.Key] = cmd.Value;
        }

        LogConfigChanged(cmd.Topic, cmd.Key, cmd.Value);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Unknown command type {CommandType} at index {Index}")]
    private partial void LogUnknownCommandType(MetadataCommandType commandType, long index);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error applying {CommandType} at index {Index}")]
    private partial void LogApplyError(MetadataCommandType commandType, long index, Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Restored snapshot: {Brokers} brokers, {Topics} topics, {Partitions} partitions")]
    private partial void LogSnapshotRestored(int brokers, int topics, int partitions);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} registered at {Host}:{Port}")]
    private partial void LogBrokerRegistered(int brokerId, string host, int port);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} removed")]
    private partial void LogBrokerRemoved(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Topic {Name} created with {Partitions} partitions, RF={ReplicationFactor}")]
    private partial void LogTopicCreated(string name, int partitions, short replicationFactor);

    [LoggerMessage(Level = LogLevel.Information, Message = "Topic {Name} deleted")]
    private partial void LogTopicDeleted(string name);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Partition {Topic}-{Partition} assigned to [{Replicas}]")]
    private partial void LogPartitionAssigned(string topic, int partition, string replicas);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ISR changed for {Topic}-{Partition}: [{Isr}]")]
    private partial void LogIsrChanged(string topic, int partition, string isr);

    [LoggerMessage(Level = LogLevel.Information, Message = "Leader changed for {Topic}-{Partition}: leader={Leader}, epoch={Epoch}")]
    private partial void LogLeaderChanged(string topic, int partition, int leader, int epoch);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Config changed for {Topic}: {Key}={Value}")]
    private partial void LogConfigChanged(string topic, string key, string value);
}

#region Metadata Commands

/// <summary>
/// Command to register a new broker. #72 Inc5 appended <c>IncarnationId</c>, <c>InterBrokerProtocol</c>
/// and <c>ReplicationPort</c> (all with defaults) so the raft-log.bin FRAME is unchanged and pre-Inc5
/// entries still deserialize — an older entry replays with IncarnationId=empty, level=0 (Kafka wire)
/// and ReplicationPort=0 (derive from the client port). The broker epoch is NOT carried here; it is
/// the committed log index at apply time.
/// </summary>
public sealed record BrokerRegisteredCommand(
    int BrokerId, string Host, int Port, string? Rack,
    Guid IncarnationId = default, short InterBrokerProtocol = 0, int ReplicationPort = 0);

/// <summary>
/// Command to remove a broker.
/// </summary>
public sealed record BrokerRemovedCommand(int BrokerId);

/// <summary>
/// Command to create a topic.
/// </summary>
public sealed record TopicCreatedCommand(
    string Name,
    Guid TopicId,
    int PartitionCount,
    short ReplicationFactor,
    Dictionary<string, string>? Config
);

/// <summary>
/// Command to delete a topic.
/// </summary>
public sealed record TopicDeletedCommand(string Name);

/// <summary>
/// Command to assign replicas to a partition.
/// </summary>
public sealed record PartitionAssignedCommand(
    string Topic,
    int Partition,
    List<int> Replicas,
    int MinIsr
);

/// <summary>
/// Command to change ISR for a partition.
/// </summary>
public sealed record IsrChangedCommand(
    string Topic,
    int Partition,
    List<int> Isr
);

/// <summary>
/// Command to change leader for a partition.
/// </summary>
public sealed record LeaderChangedCommand(
    string Topic,
    int Partition,
    int NewLeader,
    int LeaderEpoch
);

/// <summary>
/// Command to change topic configuration.
/// </summary>
public sealed record ConfigChangedCommand(
    string Topic,
    string Key,
    string Value
);

#endregion

#region Snapshot Types

/// <summary>
/// Snapshot of all cluster metadata for Raft log compaction.
/// </summary>
internal sealed class MetadataSnapshot
{
    public List<BrokerNode> Brokers { get; set; } = [];
    public List<TopicMetadata> Topics { get; set; } = [];
    public List<PartitionStateSnapshot> PartitionStates { get; set; } = [];
    public int ControllerId { get; set; }
    public int ControllerEpoch { get; set; }
}

/// <summary>
/// Partition state for snapshot.
/// </summary>
internal sealed class PartitionStateSnapshot
{
    public required string Topic { get; set; }
    public int Partition { get; set; }
    public required PartitionState State { get; set; }
}

#endregion

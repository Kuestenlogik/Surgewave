using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// LoggerMessage declarations for ClusterController.
/// </summary>
public sealed partial class ClusterController
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Discovered broker {BrokerId} at {Host}:{Port}")]
    private partial void LogDiscoveredBroker(int brokerId, string host, int port);

    [LoggerMessage(Level = LogLevel.Information, Message = "This broker ({BrokerId}) became controller (epoch={Epoch})")]
    private partial void LogBecameController(int brokerId, int epoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Controller elected: broker {BrokerId}")]
    private partial void LogControllerElected(int brokerId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Not controller, cannot perform {Operation}")]
    private partial void LogNotController(string operation);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in controller loop")]
    private partial void LogControllerLoopError(Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No brokers available to assign topic {Topic}")]
    private partial void LogNoBrokersAvailable(string topic);

    [LoggerMessage(Level = LogLevel.Information, Message = "Partition {Topic}-{Partition} assigned to replicas [{Replicas}]")]
    private partial void LogPartitionAssigned(string topic, int partition, string replicas);

    [LoggerMessage(Level = LogLevel.Information, Message = "Topic {Topic} created with {Partitions} partitions and replication factor {ReplicationFactor}")]
    private partial void LogTopicCreated(string topic, int partitions, short replicationFactor);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Partition {Topic}-{Partition} not found")]
    private partial void LogPartitionNotFound(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Unclean leader election for {Topic}-{Partition}: new leader {NewLeader}")]
    private partial void LogUncleanLeaderElection(string topic, int partition, int newLeader);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No replicas available for {Topic}-{Partition}")]
    private partial void LogNoReplicasAvailable(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Information, Message = "Leader elected for {Topic}-{Partition}: {OldLeader} -> {NewLeader} (epoch={Epoch})")]
    private partial void LogLeaderElected(string topic, int partition, int oldLeader, int newLeader, int epoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Leader imbalance detected: {Count} partitions not on preferred leader")]
    private partial void LogLeaderImbalanceDetected(int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker {BrokerId} failure detected via heartbeat")]
    private partial void LogBrokerFailureDetected(int brokerId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Controller broker {BrokerId} failed, attempting re-election")]
    private partial void LogControllerFailed(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Removed broker {BrokerId} from ISR for {Topic}-{Partition}")]
    private partial void LogRemovedFromIsr(string topic, int partition, int brokerId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Leader broker {BrokerId} failed for {Topic}-{Partition}, electing new leader")]
    private partial void LogLeaderFailedForPartition(string topic, int partition, int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} failure handled, {AffectedCount} partitions affected")]
    private partial void LogBrokerFailureHandled(int brokerId, int affectedCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} recovery detected")]
    private partial void LogBrokerRecoveryDetected(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Raft consensus mode enabled")]
    private partial void LogRaftModeEnabled();

    [LoggerMessage(Level = LogLevel.Information, Message = "Became Raft leader, now controller (brokerId={BrokerId}, epoch={Epoch})")]
    private partial void LogBecameRaftLeader(int brokerId, int epoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Lost Raft leadership, new leader is {LeaderId}")]
    private partial void LogLostRaftLeadership(int leaderId);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in Raft leader watch loop")]
    private partial void LogRaftWatchError(Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Raft commit timeout for {CommandType} at index {Index}")]
    private partial void LogRaftCommitTimeout(string commandType, long index);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} registered via Raft at {Host}:{Port}")]
    private partial void LogBrokerRegisteredViaRaft(int brokerId, string host, int port);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Controller detected network isolation - stepping down gracefully")]
    private partial void LogControllerIsolationDetected();

    // Graceful shutdown logging
    [LoggerMessage(Level = LogLevel.Information, Message = "Graceful shutdown started for broker {BrokerId}")]
    private partial void LogGracefulShutdownStarted(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Transferring leadership for {Count} partitions")]
    private partial void LogTransferringPartitionLeadership(int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Partition {Topic}-{Partition} leadership transferred to broker {NewLeader}")]
    private partial void LogPartitionLeadershipTransferred(string topic, int partition, int newLeader);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No eligible leader in ISR for {Topic}-{Partition}")]
    private partial void LogNoEligibleLeaderForPartition(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Raft leadership transfer failed or timed out")]
    private partial void LogRaftLeadershipTransferFailed();

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker removal will be detected via heartbeat timeout")]
    private partial void LogBrokerRemovalWillBeDetected();

    [LoggerMessage(Level = LogLevel.Information, Message = "Controller removal will be handled by new Raft leader")]
    private partial void LogControllerRemovalViaNewLeader();

    [LoggerMessage(Level = LogLevel.Information, Message = "Graceful shutdown completed for broker {BrokerId} (success={Success})")]
    private partial void LogGracefulShutdownCompleted(int brokerId, bool success);

    // Auto-rebalancing logging
    [LoggerMessage(Level = LogLevel.Debug, Message = "Rebalance skipped: {Count} active reassignments in progress")]
    private partial void LogRebalanceSkippedActiveReassignments(int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster imbalance detected: state={State}, leaderImbalance={LeaderImbalance:F3}, replicaImbalance={ReplicaImbalance:F3}")]
    private partial void LogClusterImbalanceDetected(string state, double leaderImbalance, double replicaImbalance);

    [LoggerMessage(Level = LogLevel.Information, Message = "Executing rebalance plan: {LeaderElections} leader elections, {Reassignments} replica reassignments")]
    private partial void LogExecutingRebalancePlan(int leaderElections, int reassignments);

    [LoggerMessage(Level = LogLevel.Information, Message = "Triggering automatic rebalance after broker {BrokerId} failure")]
    private partial void LogTriggeringRebalanceAfterFailure(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Triggering automatic rebalance after broker {BrokerId} recovery")]
    private partial void LogTriggeringRebalanceAfterRecovery(int brokerId);
}

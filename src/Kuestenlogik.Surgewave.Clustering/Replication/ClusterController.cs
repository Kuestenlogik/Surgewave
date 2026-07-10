using System.Text.Json;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Interface for cluster topic creation operations.
/// Allows handlers to create topics through the cluster controller when in cluster mode.
/// </summary>
public interface IClusterTopicCreator
{
    /// <summary>
    /// Creates a topic with the specified configuration, distributing replicas across cluster nodes.
    /// </summary>
    Task<bool> CreateTopicAsync(string topic, int partitionCount, short replicationFactor, CancellationToken ct);

    /// <summary>
    /// Whether this broker is currently the controller and can create topics.
    /// </summary>
    bool IsController { get; }
}

/// <summary>
/// Manages cluster coordination including leader election and partition assignment.
/// In a multi-broker setup, one broker becomes the controller.
/// Supports both simple "lowest broker ID" strategy and Raft consensus.
/// </summary>
public sealed partial class ClusterController : IAsyncDisposable, IClusterTopicCreator, IIsrUpdateApplier
{
    private readonly ILogger<ClusterController> _logger;
    private readonly ClusterState _clusterState;
    private readonly ReplicaManager _replicaManager;
    private readonly ClusteringConfig _config;
    private readonly ReplicaAssignmentStrategy _replicaAssignment;
    private HeartbeatManager? _heartbeatManager;
    private RaftNode? _raftNode;
    private MetadataUpdateClient? _metadataUpdateClient;
    private IControllerReplicaRpc? _controllerClient;

    private CancellationTokenSource? _cts;
    private Task? _controllerTask;
    private Task? _raftLeaderWatchTask;
    private bool _isController;

    // Auto-rebalancing components (set externally when available)
    private ClusterBalancer? _clusterBalancer;
    private PartitionReassignmentManager? _reassignmentManager;

    public bool IsController => _isController;

    /// <summary>
    /// Returns true if Raft consensus is enabled and this broker is the Raft leader.
    /// </summary>
    public bool IsRaftLeader => _raftNode?.IsLeader ?? false;

    /// <summary>
    /// Get the heartbeat manager for broker health tracking.
    /// </summary>
    public HeartbeatManager? HeartbeatManager => _heartbeatManager;

    public ClusterController(
        ILogger<ClusterController> logger,
        ClusterState clusterState,
        ReplicaManager replicaManager,
        ClusteringConfig config)
    {
        _logger = logger;
        _clusterState = clusterState;
        _replicaManager = replicaManager;
        _config = config;
        _replicaAssignment = new ReplicaAssignmentStrategy(clusterState);
    }

    /// <summary>
    /// Set the heartbeat manager and wire up failure detection handlers.
    /// </summary>
    public void SetHeartbeatManager(HeartbeatManager heartbeatManager)
    {
        _heartbeatManager = heartbeatManager;
        _heartbeatManager.OnBrokerFailed += async (_, e) => await HandleBrokerFailedAsync(e.BrokerId);
        _heartbeatManager.OnBrokerRecovered += async (_, e) => await HandleBrokerRecoveredAsync(e.BrokerId);
    }

    /// <summary>
    /// Set the Raft node for consensus-based controller election.
    /// When Raft is used, the Raft leader becomes the controller.
    /// </summary>
    public void SetRaftNode(RaftNode raftNode)
    {
        _raftNode = raftNode;

        // Subscribe to quorum lost event for controller-level handling
        _raftNode.OnQuorumLost += HandleRaftQuorumLost;
    }

    private void HandleRaftQuorumLost(object? sender, EventArgs e)
    {
        LogControllerIsolationDetected();

        // The RaftNode has already stepped down, the watch loop will update controller state.
        // This handler is for any additional cleanup or notifications needed.
        _isController = false;
    }

    /// <summary>
    /// Set the metadata update client for propagating metadata changes to remote brokers.
    /// </summary>
    public void SetMetadataUpdateClient(MetadataUpdateClient client)
    {
        _metadataUpdateClient = client;
    }

    /// <summary>
    /// Set the controller client for sending Controller API requests to brokers.
    /// </summary>
    public void SetControllerClient(IControllerReplicaRpc client)
    {
        _controllerClient = client;
    }

    /// <summary>
    /// Set the cluster balancer for automatic partition rebalancing.
    /// </summary>
    public void SetClusterBalancer(ClusterBalancer balancer)
    {
        _clusterBalancer = balancer;
    }

    /// <summary>
    /// Set the partition reassignment manager for executing rebalance plans.
    /// </summary>
    public void SetReassignmentManager(PartitionReassignmentManager manager)
    {
        _reassignmentManager = manager;
    }

    /// <summary>
    /// Get the Raft node if Raft consensus is enabled.
    /// </summary>
    public RaftNode? RaftNode => _raftNode;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Register this broker in cluster state
        var localBroker = new BrokerNode
        {
            BrokerId = _config.BrokerId,
            Host = _config.Host,
            Port = _config.Port,
            Rack = _config.Rack
        };
        _clusterState.AddBroker(localBroker);
        _clusterState.LocalBrokerId = _config.BrokerId;

        // Parse cluster nodes from config
        await InitializeClusterNodesAsync();

        if (_config.UseRaftConsensus && _raftNode != null)
        {
            // Raft mode: start Raft node and watch for leadership changes
            await _raftNode.StartAsync(cancellationToken);
            _raftLeaderWatchTask = Task.Run(() => RaftLeaderWatchLoopAsync(_cts.Token), _cts.Token);
            LogRaftModeEnabled();
        }
        else
        {
            // Legacy mode: lowest broker ID becomes controller
            await TryBecomeControllerAsync(cancellationToken);

            // Start controller loop if we're the controller
            if (_isController)
            {
                _controllerTask = Task.Run(() => ControllerLoopAsync(_cts.Token), _cts.Token);
            }
        }
    }

    /// <summary>
    /// Watches for Raft leadership changes and updates controller state.
    /// </summary>
    private async Task RaftLeaderWatchLoopAsync(CancellationToken ct)
    {
        var wasLeader = false;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(100, ct); // Check every 100ms

                if (_raftNode == null) continue;

                var isLeader = _raftNode.IsLeader;

                if (isLeader && !wasLeader)
                {
                    // Became Raft leader = became controller
                    _isController = true;
                    _clusterState.ControllerId = _config.BrokerId;
                    _clusterState.ControllerEpoch++;
                    LogBecameRaftLeader(_config.BrokerId, _clusterState.ControllerEpoch);

                    // Register this broker via Raft
                    _ = RegisterBrokerViaRaftAsync(_config.BrokerId, _config.Host, _config.Port, _config.Rack, ct);

                    // Start controller loop
                    _controllerTask = Task.Run(() => ControllerLoopAsync(ct), ct);
                }
                else if (!isLeader && wasLeader)
                {
                    // Lost Raft leadership = lost controller role
                    _isController = false;
                    var leaderId = _raftNode.LeaderId;
                    if (leaderId.HasValue)
                    {
                        _clusterState.ControllerId = leaderId.Value;
                    }
                    LogLostRaftLeadership(_raftNode.LeaderId ?? -1);
                }
                else if (!isLeader && _raftNode.LeaderId.HasValue)
                {
                    // Update controller ID if we know who the leader is
                    _clusterState.ControllerId = _raftNode.LeaderId.Value;
                }

                wasLeader = isLeader;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogRaftWatchError(ex);
            }
        }
    }

    /// <summary>
    /// Initiates graceful shutdown of this broker in the cluster.
    /// Transfers partition leadership, removes from cluster state, and steps down from Raft leadership.
    /// </summary>
    /// <param name="timeout">Maximum time to wait for graceful shutdown</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>True if shutdown was graceful, false if timed out</returns>
    public async Task<bool> GracefulShutdownAsync(TimeSpan timeout, CancellationToken ct = default)
    {
        LogGracefulShutdownStarted(_config.BrokerId);
        var deadline = DateTimeOffset.UtcNow + timeout;
        var success = true;

        // Step 1: Transfer partition leadership for partitions where we're leader
        var partitionsToTransfer = new List<TopicPartition>();
        foreach (var (tp, state) in _clusterState.PartitionStates)
        {
            if (state.LeaderBrokerId == _config.BrokerId)
            {
                partitionsToTransfer.Add(tp);
            }
        }

        if (partitionsToTransfer.Count > 0)
        {
            LogTransferringPartitionLeadership(partitionsToTransfer.Count);

            foreach (var tp in partitionsToTransfer)
            {
                if (ct.IsCancellationRequested || DateTimeOffset.UtcNow >= deadline)
                    break;

                // Elect a new leader from ISR (excluding ourselves)
                var state = _clusterState.GetPartitionState(tp);
                if (state != null)
                {
                    var eligibleLeaders = state.Isr.Where(b => b != _config.BrokerId).ToList();
                    if (eligibleLeaders.Count > 0)
                    {
                        var newLeader = eligibleLeaders.First();
                        await ElectLeaderAsync(tp, newLeader, ct);
                        LogPartitionLeadershipTransferred(tp.Topic, tp.Partition, newLeader);
                    }
                    else
                    {
                        LogNoEligibleLeaderForPartition(tp.Topic, tp.Partition);
                    }
                }
            }
        }

        // Step 2: Step down from Raft leadership if we're the leader
        if (_raftNode != null && _raftNode.IsLeader)
        {
            var raftTimeout = deadline - DateTimeOffset.UtcNow;
            if (raftTimeout > TimeSpan.Zero)
            {
                var raftShutdown = await _raftNode.GracefulShutdownAsync(raftTimeout, ct);
                if (!raftShutdown)
                {
                    LogRaftLeadershipTransferFailed();
                    success = false;
                }
            }
        }

        // Step 3: Remove ourselves from cluster state via Raft (if we can reach the controller)
        if (_raftNode != null && _raftNode.LeaderId.HasValue && _raftNode.LeaderId.Value != _config.BrokerId)
        {
            // We're not the leader, so we need to notify the controller via RPC
            // For now, the controller will detect our departure via heartbeat timeout
            LogBrokerRemovalWillBeDetected();
        }
        else if (_isController && _raftNode != null)
        {
            // We were the controller, removal will happen via new controller
            LogControllerRemovalViaNewLeader();
        }

        LogGracefulShutdownCompleted(_config.BrokerId, success);
        return success;
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is not null)
        {
            try { await _cts.CancelAsync(); } catch (ObjectDisposedException) { }
        }

        if (_controllerTask != null)
        {
            try { await _controllerTask; } catch (OperationCanceledException) { }
        }

        if (_raftLeaderWatchTask != null)
        {
            try { await _raftLeaderWatchTask; } catch (OperationCanceledException) { }
        }

        if (_raftNode != null)
        {
            await _raftNode.DisposeAsync();
        }

        _cts?.Dispose();
        _cts = null;
    }

    private Task InitializeClusterNodesAsync()
    {
        if (string.IsNullOrEmpty(_config.ClusterNodes))
            return Task.CompletedTask;

        // Parse comma-separated list: "brokerId:host:port" or "brokerId:host:port:replicationPort"
        var nodes = _config.ClusterNodes.Split(',', StringSplitOptions.RemoveEmptyEntries);
        foreach (var node in nodes)
        {
            var parts = node.Trim().Split(':');
            if (parts.Length >= 3 && int.TryParse(parts[0], out var brokerId) && int.TryParse(parts[2], out var port))
            {
                // Optional 4th part: replication port (defaults to port + 1000 if not specified)
                int? replicationPort = null;
                if (parts.Length >= 4 && int.TryParse(parts[3], out var replPort))
                {
                    replicationPort = replPort;
                }

                var broker = replicationPort.HasValue
                    ? new BrokerNode { BrokerId = brokerId, Host = parts[1], Port = port, ReplicationPort = replicationPort.Value }
                    : new BrokerNode { BrokerId = brokerId, Host = parts[1], Port = port };

                _clusterState.AddBroker(broker);
                LogDiscoveredBroker(brokerId, parts[1], port);
            }
        }

        return Task.CompletedTask;
    }

    private Task TryBecomeControllerAsync(CancellationToken ct)
    {
        // Simple strategy: lowest broker ID becomes controller
        var lowestBrokerId = _clusterState.Brokers.Keys.Min();

        if (_config.BrokerId == lowestBrokerId)
        {
            _isController = true;
            _clusterState.ControllerId = _config.BrokerId;
            _clusterState.ControllerEpoch++;
            LogBecameController(_config.BrokerId, _clusterState.ControllerEpoch);
        }
        else
        {
            _clusterState.ControllerId = lowestBrokerId;
            LogControllerElected(lowestBrokerId);
        }

        return Task.CompletedTask;
    }

    private async Task ControllerLoopAsync(CancellationToken ct)
    {
        var checkInterval = TimeSpan.FromSeconds(
            Math.Max(5, _config.RebalanceCheckIntervalSeconds));

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(checkInterval, ct);

                // Step 1: Preferred leader elections (fast, no data movement)
                if (_config.AllowAutoLeaderRebalance)
                {
                    await CheckLeaderBalanceAsync(ct);
                }

                // Step 2: Full cluster rebalancing (replica redistribution)
                if (_config.AutoRebalanceEnabled)
                {
                    await MaybeRebalanceClusterAsync(ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogControllerLoopError(ex);
            }
        }
    }

    /// <summary>
    /// Evaluates cluster balance and triggers replica reassignment if needed.
    /// Uses ClusterBalancer to detect imbalance and PartitionReassignmentManager to execute moves.
    /// </summary>
    private async Task MaybeRebalanceClusterAsync(CancellationToken ct)
    {
        if (_clusterBalancer == null || _reassignmentManager == null)
            return;

        // Skip if there are active reassignments already in progress
        var activeReassignments = _reassignmentManager.GetActiveReassignments();
        if (activeReassignments.Count > 0)
        {
            LogRebalanceSkippedActiveReassignments(activeReassignments.Count);
            return;
        }

        // Check if rebalancing is needed
        if (!_clusterBalancer.IsRebalanceNeeded())
            return;

        var status = _clusterBalancer.GetBalanceStatus();
        LogClusterImbalanceDetected(
            status.State.ToString(),
            status.LeaderImbalanceRatio,
            status.ReplicaImbalanceRatio);

        // Generate rebalance plan
        var plan = _clusterBalancer.GenerateRebalancePlan();

        // Execute leader elections from the plan
        foreach (var election in plan.LeaderElections)
        {
            var tp = new TopicPartition { Topic = election.Topic, Partition = election.Partition };
            await ElectLeaderAsync(tp, election.NewLeader, ct);
        }

        // Execute replica reassignments if the plan includes them
        if (plan.ReassignmentPlan != null && plan.ReassignmentPlan.Partitions.Count > 0)
        {
            // Respect max concurrent reassignments
            var limited = plan.ReassignmentPlan;
            if (limited.Partitions.Count > _config.ReassignmentMaxConcurrent)
            {
                limited = new ReassignmentPlan
                {
                    Version = 1,
                    Partitions = limited.Partitions.Take(_config.ReassignmentMaxConcurrent).ToList()
                };
            }

            LogExecutingRebalancePlan(plan.LeaderElections.Count, limited.Partitions.Count);
            await _reassignmentManager.ExecuteReassignmentAsync(limited, ct);
        }
        else if (plan.LeaderElections.Count > 0)
        {
            LogExecutingRebalancePlan(plan.LeaderElections.Count, 0);
        }

        // Clean up completed reassignments
        _reassignmentManager.ClearCompleted();
    }

    /// <summary>
    /// Create a new topic with partition and replica assignment.
    /// </summary>
    public async Task<bool> CreateTopicAsync(string topic, int partitionCount, short replicationFactor, CancellationToken ct)
    {
        if (!_isController)
        {
            LogNotController("CreateTopic");
            return false;
        }

        // When Raft is enabled, propose through Raft log for replication
        if (_config.UseRaftConsensus && _raftNode != null)
        {
            return await CreateTopicViaRaftAsync(topic, partitionCount, replicationFactor, ct);
        }

        // Legacy mode: apply directly
        return await CreateTopicDirectAsync(topic, partitionCount, replicationFactor, ct);
    }

    /// <summary>
    /// Create topic via Raft consensus - proposes to Raft log and waits for commit.
    /// </summary>
    private async Task<bool> CreateTopicViaRaftAsync(string topic, int partitionCount, short replicationFactor, CancellationToken ct)
    {
        // Propose TopicCreated command with newly generated TopicId
        var topicId = Guid.NewGuid();
        var command = new TopicCreatedCommand(topic, topicId, partitionCount, replicationFactor, null);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.TopicCreatedCommand);

        var index = await _raftNode!.ProposeAsync(MetadataCommandType.TopicCreated, data, ct);
        if (index < 0)
        {
            LogNotController("CreateTopic (Raft propose failed)");
            return false;
        }

        // Wait for commit
        var committed = await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(10), ct);
        if (!committed)
        {
            LogRaftCommitTimeout("TopicCreated", index);
            return false;
        }

        // Now propose partition assignments
        var brokerIds = _clusterState.Brokers.Keys.OrderBy(id => id).ToList();
        if (brokerIds.Count == 0)
        {
            LogNoBrokersAvailable(topic);
            return false;
        }

        for (int partition = 0; partition < partitionCount; partition++)
        {
            var replicas = _replicaAssignment.AssignReplicas(brokerIds, partition, replicationFactor);
            var assignCmd = new PartitionAssignedCommand(topic, partition, replicas, _config.MinInSyncReplicas);
            var assignData = JsonSerializer.SerializeToUtf8Bytes(assignCmd, ClusteringJsonContext.Default.PartitionAssignedCommand);

            var assignIndex = await _raftNode.ProposeAsync(MetadataCommandType.PartitionAssigned, assignData, ct);
            if (assignIndex > 0)
            {
                await _raftNode.WaitForCommitAsync(assignIndex, TimeSpan.FromSeconds(5), ct);
            }

            // Apply local replica manager state
            var tp = new TopicPartition { Topic = topic, Partition = partition };
            foreach (var brokerId in replicas)
            {
                if (brokerId == _config.BrokerId)
                {
                    var isLeader = replicas[0] == brokerId;
                    if (isLeader)
                    {
                        await _replicaManager.BecomeLeaderAsync(tp, 1, ct);
                    }
                    else
                    {
                        await _replicaManager.BecomeFollowerAsync(tp, replicas[0], 1, ct);
                    }
                }
            }
        }

        LogTopicCreated(topic, partitionCount, replicationFactor);
        return true;
    }

    /// <summary>
    /// Create topic directly (legacy mode without Raft).
    /// </summary>
    private async Task<bool> CreateTopicDirectAsync(string topic, int partitionCount, short replicationFactor, CancellationToken ct)
    {
        // Create topic metadata
        var topicMetadata = new TopicMetadata
        {
            Name = topic,
            TopicId = Guid.NewGuid(),
            PartitionCount = partitionCount,
            ReplicationFactor = replicationFactor,
            Config = new Dictionary<string, string>(),
            CreatedAt = DateTime.UtcNow
        };
        _clusterState.AddTopic(topicMetadata);

        // Broadcast topic creation to all brokers
        if (_metadataUpdateClient != null)
        {
            var topicCmd = new TopicCreatedCommand(topic, topicMetadata.TopicId, partitionCount, replicationFactor, null);
            var topicData = JsonSerializer.SerializeToUtf8Bytes(topicCmd, ClusteringJsonContext.Default.TopicCreatedCommand);
            await _metadataUpdateClient.BroadcastMetadataUpdateAsync(MetadataCommandType.TopicCreated, topicData, ct);
        }

        // Assign partitions to brokers
        var brokerIds = _clusterState.Brokers.Keys.OrderBy(id => id).ToList();
        if (brokerIds.Count == 0)
        {
            LogNoBrokersAvailable(topic);
            return false;
        }

        for (int partition = 0; partition < partitionCount; partition++)
        {
            var tp = new TopicPartition { Topic = topic, Partition = partition };

            // Round-robin assignment with rack awareness
            var replicas = _replicaAssignment.AssignReplicas(brokerIds, partition, replicationFactor);

            _clusterState.AssignReplicas(tp, replicas, _config.MinInSyncReplicas);

            LogPartitionAssigned(topic, partition, string.Join(",", replicas));

            // Broadcast partition assignment to all brokers
            if (_metadataUpdateClient != null)
            {
                var assignCmd = new PartitionAssignedCommand(topic, partition, replicas, _config.MinInSyncReplicas);
                var assignData = JsonSerializer.SerializeToUtf8Bytes(assignCmd, ClusteringJsonContext.Default.PartitionAssignedCommand);
                await _metadataUpdateClient.BroadcastMetadataUpdateAsync(MetadataCommandType.PartitionAssigned, assignData, ct);
            }

            // Handle local broker assignments
            foreach (var brokerId in replicas)
            {
                if (brokerId == _config.BrokerId)
                {
                    // Local broker
                    var isLeader = replicas[0] == brokerId;
                    if (isLeader)
                    {
                        await _replicaManager.BecomeLeaderAsync(tp, 1, ct);
                    }
                    else
                    {
                        await _replicaManager.BecomeFollowerAsync(tp, replicas[0], 1, ct);
                    }
                }
            }

            // Broadcast LeaderAndIsr to remote brokers for this partition.
            // Awaited (not fire-and-forget) so the controller's reelection
            // returns only after the surviving brokers have updated their
            // local _clusterState — otherwise a Metadata request landing on
            // a follower right after reelection still sees the stale leader.
            if (_controllerClient != null)
            {
                var partitionState = _clusterState.GetPartitionState(tp);
                if (partitionState != null)
                {
                    await _controllerClient.SendLeaderAndIsrAsync([(tp, partitionState)], ct).ConfigureAwait(false);
                }
            }
        }

        LogTopicCreated(topic, partitionCount, replicationFactor);
        return true;
    }

    #region Raft-based Metadata Operations

    /// <summary>
    /// Register a broker via Raft consensus.
    /// </summary>
    public async Task<bool> RegisterBrokerViaRaftAsync(int brokerId, string host, int port, string? rack, CancellationToken ct)
    {
        if (_raftNode == null || !_raftNode.IsLeader)
        {
            return false;
        }

        var command = new BrokerRegisteredCommand(brokerId, host, port, rack);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.BrokerRegisteredCommand);

        var index = await _raftNode.ProposeAsync(MetadataCommandType.BrokerRegistered, data, ct);
        if (index < 0) return false;

        var committed = await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct);
        if (committed)
        {
            LogBrokerRegisteredViaRaft(brokerId, host, port);
        }
        return committed;
    }

    /// <summary>
    /// Remove a broker via Raft consensus.
    /// </summary>
    public async Task<bool> RemoveBrokerViaRaftAsync(int brokerId, CancellationToken ct)
    {
        if (_raftNode == null || !_raftNode.IsLeader)
        {
            return false;
        }

        var command = new BrokerRemovedCommand(brokerId);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.BrokerRemovedCommand);

        var index = await _raftNode.ProposeAsync(MetadataCommandType.BrokerRemoved, data, ct);
        if (index < 0) return false;

        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct);
    }

    /// <summary>
    /// Update ISR for a partition via Raft consensus.
    /// </summary>
    public async Task<bool> UpdateIsrViaRaftAsync(TopicPartition tp, List<int> isr, CancellationToken ct)
    {
        if (_raftNode == null || !_raftNode.IsLeader)
        {
            return false;
        }

        var command = new IsrChangedCommand(tp.Topic, tp.Partition, isr);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.IsrChangedCommand);

        var index = await _raftNode.ProposeAsync(MetadataCommandType.IsrChanged, data, ct);
        if (index < 0) return false;

        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct);
    }

    /// <summary>
    /// Elect a new leader via Raft consensus.
    /// </summary>
    private async Task<bool> ElectLeaderViaRaftAsync(TopicPartition tp, int newLeader, int leaderEpoch, CancellationToken ct)
    {
        if (_raftNode == null || !_raftNode.IsLeader)
        {
            return false;
        }

        var command = new LeaderChangedCommand(tp.Topic, tp.Partition, newLeader, leaderEpoch);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.LeaderChangedCommand);

        var index = await _raftNode.ProposeAsync(MetadataCommandType.LeaderChanged, data, ct);
        if (index < 0) return false;

        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct);
    }

    /// <summary>
    /// Delete a topic via Raft consensus.
    /// </summary>
    public async Task<bool> DeleteTopicViaRaftAsync(string topicName, CancellationToken ct)
    {
        if (_raftNode == null || !_raftNode.IsLeader)
        {
            return false;
        }

        var command = new TopicDeletedCommand(topicName);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.TopicDeletedCommand);

        var index = await _raftNode.ProposeAsync(MetadataCommandType.TopicDeleted, data, ct);
        if (index < 0) return false;

        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct);
    }

    /// <summary>
    /// Update topic configuration via Raft consensus.
    /// </summary>
    public async Task<bool> UpdateConfigViaRaftAsync(string topic, string key, string value, CancellationToken ct)
    {
        if (_raftNode == null || !_raftNode.IsLeader)
        {
            return false;
        }

        var command = new ConfigChangedCommand(topic, key, value);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.ConfigChangedCommand);

        var index = await _raftNode.ProposeAsync(MetadataCommandType.ConfigChanged, data, ct);
        if (index < 0) return false;

        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct);
    }

    #endregion

    /// <summary>
    /// Elect a new leader for a partition.
    /// </summary>
    public async Task<bool> ElectLeaderAsync(TopicPartition tp, int? preferredLeader = null, CancellationToken ct = default)
    {
        if (!_isController)
        {
            LogNotController("ElectLeader");
            return false;
        }

        var partitionState = _clusterState.GetPartitionState(tp);
        if (partitionState == null)
        {
            LogPartitionNotFound(tp.Topic, tp.Partition);
            return false;
        }

        var oldLeader = partitionState.LeaderBrokerId;
        var newLeaderEpoch = partitionState.LeaderEpoch + 1;

        // Determine new leader
        int newLeader;
        if (preferredLeader.HasValue && partitionState.Isr.Contains(preferredLeader.Value))
        {
            newLeader = preferredLeader.Value;
        }
        else if (partitionState.Isr.Count > 0)
        {
            // Pick first ISR member
            newLeader = partitionState.Isr[0];
        }
        else if (partitionState.Replicas.Count > 0)
        {
            // Unclean election from any alive replica
            var aliveReplica = _heartbeatManager != null
                ? partitionState.Replicas.FirstOrDefault(r => _heartbeatManager.IsBrokerAlive(r), -1)
                : partitionState.Replicas[0];

            if (aliveReplica < 0)
            {
                LogNoReplicasAvailable(tp.Topic, tp.Partition);
                return false;
            }

            newLeader = aliveReplica;
            LogUncleanLeaderElection(tp.Topic, tp.Partition, newLeader);
        }
        else
        {
            LogNoReplicasAvailable(tp.Topic, tp.Partition);
            return false;
        }

        // When Raft is enabled, replicate leader change via Raft
        if (_config.UseRaftConsensus && _raftNode != null)
        {
            var committed = await ElectLeaderViaRaftAsync(tp, newLeader, newLeaderEpoch, ct);
            if (!committed)
            {
                LogRaftCommitTimeout("LeaderChanged", 0);
                return false;
            }
        }
        else
        {
            // Update partition state directly
            _clusterState.ElectLeader(tp, newLeader);
        }

        // Notify local replica manager
        if (newLeader == _config.BrokerId)
        {
            await _replicaManager.BecomeLeaderAsync(tp, newLeaderEpoch, ct);
        }
        else if (partitionState.Replicas.Contains(_config.BrokerId))
        {
            await _replicaManager.BecomeFollowerAsync(tp, newLeader, newLeaderEpoch, ct);
        }

        // Broadcast LeaderAndIsr to all affected brokers. Awaited so the
        // controller's ElectLeaderAsync returns only after the followers
        // have ack'd the new leader assignment; without this a Metadata
        // request landing on a follower right after reelection still sees
        // the stale leader and the producer times out before the next
        // refresh cycle picks up the change.
        if (_controllerClient != null)
        {
            var updatedState = _clusterState.GetPartitionState(tp);
            if (updatedState != null)
            {
                await _controllerClient.SendLeaderAndIsrAsync([(tp, updatedState)], ct).ConfigureAwait(false);
            }
        }

        LogLeaderElected(tp.Topic, tp.Partition, oldLeader, newLeader, newLeaderEpoch);
        return true;
    }

    /// <summary>
    /// Controller-side apply of an ISR update reported by a partition leader
    /// (AlterPartition, reverse ISR propagation #69). Mirrors ElectLeaderAsync's
    /// "mutate ClusterState, then re-broadcast LeaderAndIsr" pattern so every
    /// replica — and the Kafka Metadata this controller serves — converges to
    /// the reported ISR.
    /// </summary>
    public async Task<PartitionState?> ApplyIsrUpdateAsync(
        TopicPartition tp,
        int leaderId,
        int leaderEpoch,
        IReadOnlyList<int> newIsr,
        CancellationToken ct = default)
    {
        if (!_isController)
        {
            LogNotController("AlterPartition");
            return null;
        }

        var state = _clusterState.GetPartitionState(tp);
        if (state == null)
        {
            LogPartitionNotFound(tp.Topic, tp.Partition);
            return null;
        }

        // Fence stale leaders: a leader epoch older than what we already hold
        // means the reporter was deposed by a reelection — reject with no change.
        if (leaderEpoch < state.LeaderEpoch)
        {
            LogStaleIsrUpdate(tp.Topic, tp.Partition, leaderEpoch, state.LeaderEpoch);
            return state;
        }

        // Apply the ISR wholesale (an ISR-only change does not bump LeaderEpoch).
        if (_config.UseRaftConsensus && _raftNode != null)
        {
            await UpdateIsrViaRaftAsync(tp, newIsr.ToList(), ct);
        }
        else
        {
            _clusterState.UpdateIsr(tp, newIsr.ToList());
        }

        // Re-broadcast LeaderAndIsr so all replicas (incl. the reporting leader)
        // and the controller's own metadata view converge.
        var updated = _clusterState.GetPartitionState(tp);
        if (_controllerClient != null && updated != null)
        {
            await _controllerClient.SendLeaderAndIsrAsync([(tp, updated)], ct).ConfigureAwait(false);
        }

        LogIsrUpdateApplied(tp.Topic, tp.Partition, leaderId, string.Join(",", newIsr));
        return updated;
    }

    /// <summary>
    /// Handle broker failure detected by heartbeat manager.
    /// </summary>
    private async Task HandleBrokerFailedAsync(int failedBrokerId)
    {
        LogBrokerFailureDetected(failedBrokerId);

        if (!_isController)
        {
            // Check if the failed broker was the controller
            if (_clusterState.ControllerId == failedBrokerId)
            {
                LogControllerFailed(failedBrokerId);
                await TryBecomeControllerAsync(CancellationToken.None);

                if (_isController)
                {
                    _controllerTask = Task.Run(() => ControllerLoopAsync(_cts!.Token), _cts!.Token);
                }
            }
            return;
        }

        // As controller, handle the broker failure
        // 1. Remove from ISR for all partitions
        var affectedPartitions = new List<TopicPartition>();

        foreach (var (tp, state) in _clusterState.PartitionStates)
        {
            if (state.Isr.Contains(failedBrokerId))
            {
                _clusterState.RemoveFromIsr(tp, failedBrokerId);
                affectedPartitions.Add(tp);
                LogRemovedFromIsr(tp.Topic, tp.Partition, failedBrokerId);
            }

            // 2. If failed broker was leader, elect new leader
            if (state.LeaderBrokerId == failedBrokerId)
            {
                LogLeaderFailedForPartition(tp.Topic, tp.Partition, failedBrokerId);
                await ElectLeaderAsync(tp, ct: CancellationToken.None);
            }
        }

        LogBrokerFailureHandled(failedBrokerId, affectedPartitions.Count);

        // Trigger automatic rebalance to redistribute replicas away from failed broker
        if (_config.AutoRebalanceEnabled && _clusterBalancer != null && _reassignmentManager != null)
        {
            LogTriggeringRebalanceAfterFailure(failedBrokerId);
            _ = Task.Run(() => MaybeRebalanceClusterAsync(CancellationToken.None));
        }
    }

    /// <summary>
    /// Handle broker recovery detected by heartbeat manager.
    /// </summary>
    private async Task HandleBrokerRecoveredAsync(int recoveredBrokerId)
    {
        LogBrokerRecoveryDetected(recoveredBrokerId);

        // The broker will need to catch up via replication before rejoining ISR
        // This is handled by ReplicaManager when the broker starts fetching

        // Trigger automatic rebalance to redistribute partitions to the recovered broker
        if (_isController && _config.AutoRebalanceEnabled && _clusterBalancer != null && _reassignmentManager != null)
        {
            LogTriggeringRebalanceAfterRecovery(recoveredBrokerId);
            await MaybeRebalanceClusterAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// Check and rebalance leaders to preferred replicas.
    /// </summary>
    private async Task CheckLeaderBalanceAsync(CancellationToken ct)
    {
        var imbalanced = new List<TopicPartition>();

        foreach (var (tp, state) in _clusterState.PartitionStates)
        {
            // Skip if preferred leader is not alive
            if (_heartbeatManager != null && !_heartbeatManager.IsBrokerAlive(state.PreferredLeader))
                continue;

            if (state.LeaderBrokerId != state.PreferredLeader &&
                state.Isr.Contains(state.PreferredLeader))
            {
                imbalanced.Add(tp);
            }
        }

        if (imbalanced.Count > 0)
        {
            LogLeaderImbalanceDetected(imbalanced.Count);

            foreach (var tp in imbalanced)
            {
                var state = _clusterState.GetPartitionState(tp);
                if (state != null)
                {
                    await ElectLeaderAsync(tp, state.PreferredLeader, ct);
                }
            }
        }
    }

}

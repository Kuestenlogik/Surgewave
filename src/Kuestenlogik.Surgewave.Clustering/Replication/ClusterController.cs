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
    /// <param name="config">
    /// The topic's configuration as the client sent it (segment size, cleanup policy, retention, …),
    /// or <see langword="null"/> when the caller has none — an auto-created topic, for instance.
    /// This used to be missing from the signature, so every clustered <c>CreateTopics</c> silently
    /// dropped the client's settings while reporting success (#118 follow-up).
    /// </param>
    /// <param name="replicaAssignment">
    /// The replicas of each partition as the client chose them, the preferred leader first (#202); null to have the
    /// controller place them.
    /// </param>
    Task<bool> CreateTopicAsync(
        string topic,
        int partitionCount,
        short replicationFactor,
        Dictionary<string, string>? config,
        IReadOnlyList<IReadOnlyList<int>>? replicaAssignment,
        CancellationToken ct);

    /// <summary>
    /// Whether this broker is currently the controller and can create topics.
    /// </summary>
    bool IsController { get; }
}

/// <summary>
/// Changes a group's configs in the metadata log (#228): directly on the controller, through it from any other broker.
/// </summary>
public interface IClusterGroupConfigWriter
{
    /// <summary>Whether this broker is the controller now.</summary>
    bool IsController { get; }

    /// <summary>
    /// Sets one config of a group, or removes it with a null value. True once committed; false when no controller
    /// could take it now.
    /// </summary>
    Task<bool> SetGroupConfigAsync(string groupId, string name, string? value, CancellationToken ct);
}

/// <summary>
/// Changes a topic's configs in the metadata log (#119): directly on the controller, through it from any other broker.
/// </summary>
public interface IClusterTopicConfigWriter
{
    /// <summary>Whether this broker is the controller now.</summary>
    bool IsController { get; }

    /// <summary>
    /// Sets and removes configs of a topic as one change. True once committed; false when no controller could take it
    /// now.
    /// </summary>
    Task<bool> ChangeTopicConfigAsync(
        string topic, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed, CancellationToken ct);
}

/// <summary>
/// Manages cluster coordination including leader election and partition assignment.
/// In a multi-broker setup, one broker becomes the controller.
/// Supports both simple "lowest broker ID" strategy and Raft consensus.
/// </summary>
public sealed partial class ClusterController : IAsyncDisposable, IClusterTopicCreator, IClusterGroupConfigWriter, IClusterTopicConfigWriter, IIsrUpdateApplier, IBrokerRegistrar, IControlledShutdownCoordinator
{
    private readonly ILogger<ClusterController> _logger;
    private readonly ClusterState _clusterState;
    private readonly ReplicaManager _replicaManager;
    private readonly ClusteringConfig _config;
    private readonly ReplicaAssignmentStrategy _replicaAssignment;
    private HeartbeatManager? _heartbeatManager;
    private RaftNode? _raftNode;

    // #180 — how a departing broker that is NOT the controller gets its partitions moved.
    private IControlledShutdownRequester? _shutdownRequester;

    // #162 — which brokers report a failed log directory, and so may not lead or rejoin an ISR.
    private ClusterMembershipService? _membership;
    private ITopicCreationRequester? _topicCreationRequester;
    private IGroupConfigRequester? _groupConfigRequester;
    private ITopicConfigRequester? _topicConfigRequester;

    private CancellationTokenSource? _cts;
    private Task? _controllerTask;
    private volatile bool _isController;

    // The Raft leadership this controller acted on last, and the lock under which it follows the node (#180).
    private readonly object _leadershipLock = new();
    private RaftLeadership _followedLeadership;

    // Ends when this controller stops, and stays ended: what its background work runs under. Not read from _cts,
    // which the stop disposes and clears.
    private CancellationToken _lifetime;

    // #72 Inc5 — process-stable incarnation for THIS broker's Raft self-registration; a restart gets
    // a fresh incarnation and thus a fresh (higher) committed-index epoch on re-registration.
    private readonly Guid _raftSelfIncarnation = Guid.NewGuid();

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
        // The node has stepped down and said so through LeadershipChanged, which is where the controller role ends
        // (#180). Ending it here as well could end the reign of an election this node won in the meantime.
        LogControllerIsolationDetected();
    }

    /// <summary>
    /// Set the client this broker uses to ask the controller for a controlled shutdown (#180).
    /// Absent it, a non-controller broker leaves without handing its partitions over and the
    /// cluster waits out the heartbeat timeout.
    /// </summary>
    public void SetControlledShutdownRequester(IControlledShutdownRequester requester)
        => _shutdownRequester = requester;

    /// <summary>
    /// Set the membership authority whose heartbeats report failed log directories (#162). A broker reporting one has
    /// its leaderships moved to in-sync replicas elsewhere, leaves the ISRs, and is not elected while the directory is
    /// offline.
    /// </summary>
    public void SetMembership(ClusterMembershipService membership)
    {
        _membership = membership;
        // Off the heartbeat: the heartbeat is answered at once, and the moves commit one by one behind it.
        membership.LogDirsOffline += (brokerId, offlineDirs) => _ = Task.Run(() => HandleLogDirsOfflineAsync(brokerId));
    }

    /// <summary>
    /// Whether <paramref name="brokerId"/> may lead a partition it replicates: alive, and with its log directory online
    /// (#162). Every replica of a Surgewave broker lives in its one log directory, so an offline directory makes the
    /// broker ineligible for all of them.
    /// </summary>
    private bool IsEligibleLeader(int brokerId)
        => (_heartbeatManager?.IsBrokerAlive(brokerId) ?? true) && !HasOfflineLogDir(brokerId);

    private bool HasOfflineLogDir(int brokerId) => _membership?.HasOfflineLogDirs(brokerId) ?? false;

    /// <summary>How this broker has a topic created while another broker is the controller (#207).</summary>
    public void SetTopicCreationRequester(ITopicCreationRequester requester)
        => _topicCreationRequester = requester;

    /// <summary>How this broker has a group config changed while another broker is the controller (#228).</summary>
    public void SetGroupConfigRequester(IGroupConfigRequester requester)
        => _groupConfigRequester = requester;

    /// <summary>How this broker has a topic's config changed while another broker is the controller (#119).</summary>
    public void SetTopicConfigRequester(ITopicConfigRequester requester)
        => _topicConfigRequester = requester;

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

        // Register this broker in cluster state. Merge with a pre-registered node instead of
        // clobbering it — the embedded runtime registers the local broker with its ACTUAL bound
        // (possibly ephemeral) ports before the controller starts, and rebuilding from config would
        // reset them. The node self-advertises the highest inter-broker protocol level this build
        // speaks (#60 Inc5): the finalized level is the MIN over ALL nodes including this one, so a
        // local node left at the KafkaWire default would pin the whole cluster forever.
        var existingLocal = _clusterState.GetBroker(_config.BrokerId);
        var localBroker = new BrokerNode
        {
            BrokerId = _config.BrokerId,
            Host = existingLocal?.Host ?? _config.Host,
            Port = existingLocal?.Port ?? _config.Port,
            Rack = existingLocal?.Rack ?? _config.Rack,
            ReplicationPort = existingLocal is { HasExplicitReplicationPort: true }
                ? existingLocal.ReplicationPort
                : _config.ReplicationPort > 0 ? _config.ReplicationPort : _config.Port + 1000,
            InterBrokerProtocol = InterBrokerProtocolFeature.LocalMaxSupported
        };
        _clusterState.AddBroker(localBroker);
        _clusterState.LocalBrokerId = _config.BrokerId;

        // Parse cluster nodes from config
        await InitializeClusterNodesAsync();

        // Raft leadership IS the controller role (#163 step 3). The lowest-id election it
        // replaced could elect a second controller with no way for either to learn it had
        // been replaced — a split view with no reconciliation path.
        if (_raftNode == null)
        {
            throw new InvalidOperationException(
                "ClusterController requires a RaftNode: metadata is a replicated log and the "
                + "controller role is Raft leadership. Call SetRaftNode before StartAsync.");
        }

        // The controller role follows the node's leadership as the node announces it (#180), and from what it knows
        // already: a leader may have been heard from before this controller started.
        _lifetime = _cts.Token;
        _raftNode.LeadershipChanged += HandleRaftLeadershipChanged;
        FollowRaftLeadership();

        // The startup handshake a host waits on before it lets clients in; ends with this controller.
        _ = CatchUpWithMetadataAsync(_raftNode, _lifetime);

        await _raftNode.StartAsync(cancellationToken);
        LogRaftModeEnabled();
    }

    private void HandleRaftLeadershipChanged(RaftLeadership leadership) => FollowRaftLeadership();

    /// <summary>
    /// Brings the controller role and the controller id this broker reports in line with the Raft node's leadership
    /// (#180): this broker is the controller from the moment its node leads a term, and names the leader its node
    /// knows from the moment the node knows it.
    /// </summary>
    /// <remarks>
    /// Run on the node's <see cref="RaftNode.LeadershipChanged"/>, on the thread that made the change. A loop used to
    /// look every 100 ms instead: for that long a follower that had heard from the leader still reported no
    /// controller — and Metadata then names the broker itself (#235), or named none before that, on which an admin
    /// client waits for a controller until its request times out — a new leader was not yet controller, and a
    /// leadership lost and won again between two looks was never seen. The node is read here, under this
    /// controller's lock, and not taken from the event, so that the start and two changes running into each other
    /// cannot apply an older leadership after a newer one.
    /// </remarks>
    private void FollowRaftLeadership()
    {
        if (_raftNode is null)
            return;

        lock (_leadershipLock)
        {
            var now = _raftNode.Leadership;
            var was = _followedLeadership;
            if (now == was)
                return;

            _followedLeadership = now;

            try
            {
                if (now.IsLeader)
                {
                    // Became Raft leader = became controller (locked bump, shares the push fence).
                    var ct = _lifetime;
                    _isController = true;
                    var epoch = _clusterState.BecomeController(_config.BrokerId);
                    LogBecameRaftLeader(_config.BrokerId, epoch);

                    // The cluster's id, unless an earlier controller wrote it (#203), then this broker's
                    // registration via Raft (carry incarnation + level + replication port so the committed
                    // entry rebuilds the membership store faithfully; #72 Inc5).
                    // Off this thread, as the controller loop: it is the node's, in the middle of its election.
                    _ = Task.Run(() => TakeOfficeAsync(ct), ct);

                    // Start controller loop
                    _controllerTask = Task.Run(() => ControllerLoopAsync(ct), ct);

                    // The group coordinator topic, once the cluster can carry it (#204).
                    _ = Task.Run(() => EnsureInternalTopicsAsync(ct), ct);
                    return;
                }

                if (was.IsLeader)
                {
                    // Lost Raft leadership = lost controller role
                    _isController = false;
                    LogLostRaftLeadership(now.LeaderId ?? -1);
                }

                // The controller is the leader the node knows. While it knows none, the last one stays named.
                if (now.LeaderId is { } leaderId)
                    _clusterState.ControllerId = leaderId;
            }
            catch (Exception ex)
            {
                LogRaftLeadershipError(ex);
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

        // Step 1: Transfer partition leadership for partitions where we're leader.
        //
        // Only the controller may elect, so a departing broker that is NOT the controller cannot
        // hand its partitions over at all: the cluster only learns it is gone when the heartbeat
        // times out, and until then those partitions have a leader that has left. Kafka solves
        // this with a ControlledShutdown request to the controller; the receiving half exists here
        // (InterBrokerApiHandler.HandleControlledShutdown) but nothing sends it, so this loop is a
        // no-op on every broker except the controller itself (#180). It used to look like it
        // worked because the election result was discarded and success logged regardless.
        var partitionsToTransfer = new List<TopicPartition>();
        foreach (var (tp, state) in _clusterState.PartitionStates)
        {
            if (state.LeaderBrokerId == _config.BrokerId)
            {
                partitionsToTransfer.Add(tp);
            }
        }

        // Not the controller: ask the one that is. Electing is the controller's privilege, so
        // doing it here is a no-op — which is exactly what used to happen, silently (#180).
        if (partitionsToTransfer.Count > 0 && !_isController && _shutdownRequester is not null)
        {
            LogTransferringPartitionLeadership(partitionsToTransfer.Count);

            var remaining = await _shutdownRequester
                .RequestControlledShutdownAsync(_config.BrokerId, _clusterState.ControllerEpoch, ct)
                .ConfigureAwait(false);

            if (remaining is null)
                LogControlledShutdownUnanswered(_config.BrokerId);
            else if (remaining.Count > 0)
                LogControlledShutdownIncomplete(remaining.Count);

            partitionsToTransfer.Clear();
        }

        if (partitionsToTransfer.Count > 0)
        {
            LogTransferringPartitionLeadership(partitionsToTransfer.Count);

            // Who we handed what to, and where the decision sits in the log. A handover the
            // successor never received is not a handover (#177), so this is checked below
            // before we walk away.
            var handovers = new Dictionary<int, long>();

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
                        var elected = await ElectLeaderAtAsync(tp, newLeader, ct);
                        if (elected is null)
                        {
                            // Not "no eligible leader" — the ISR had one. Either this broker is not
                            // the controller and may not elect, or the commit did not land.
                            LogLeadershipHandoverFailed(tp.Topic, tp.Partition, newLeader);
                            continue;
                        }

                        var (electedLeader, index) = elected.Value;
                        if (!handovers.TryGetValue(electedLeader, out var known) || index > known)
                            handovers[electedLeader] = index;

                        LogPartitionLeadershipTransferred(tp.Topic, tp.Partition, electedLeader);
                    }
                    else
                    {
                        LogNoEligibleLeaderForPartition(tp.Topic, tp.Partition);
                    }
                }
            }

            await AwaitHandoverDeliveryAsync(handovers, deadline, ct);
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

    /// <summary>
    /// Blocks until every broker named in <paramref name="handovers"/> holds the log entry that
    /// makes it a leader, or until the shutdown budget runs out (#177).
    /// </summary>
    /// <remarks>
    /// The step that makes a graceful shutdown graceful. Electing a successor commits a
    /// <c>LeaderChanged</c>, and on a single-voter quorum an entry commits the instant it is
    /// appended — the leader's own log IS the majority. Replication to everyone else happens on
    /// the next heartbeat tick, which never comes once this node is gone. Without this wait the
    /// departing controller decides who takes over, tells nobody, and leaves: the partition has a
    /// new leader that does not know it, and no one left to say so.
    /// <para>
    /// Best-effort by design. A successor that stops answering costs the remaining budget and
    /// nothing more — shutdown continues either way, because refusing to leave would be worse
    /// than leaving an unconfirmed handover behind.
    /// </para>
    /// </remarks>
    private async Task AwaitHandoverDeliveryAsync(
        Dictionary<int, long> handovers, DateTimeOffset deadline, CancellationToken ct)
    {
        if (_raftNode is null || handovers.Count == 0)
            return;

        foreach (var (successor, index) in handovers)
        {
            var budget = deadline - DateTimeOffset.UtcNow;
            if (budget <= TimeSpan.Zero)
            {
                LogHandoverNotConfirmed(successor);
                continue;
            }

            var delivered = await _raftNode.WaitForReplicationAsync(index, [successor], budget, ct);
            if (delivered)
                LogHandoverConfirmed(successor, index);
            else
                LogHandoverNotConfirmed(successor);
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Whoever still waits for a broker that never caught up is told so; a controller that was never started has
        // nothing else to tell them.
        _metadataCaughtUp.TrySetCanceled();

        if (_cts is not null)
        {
            try { await _cts.CancelAsync(); } catch (ObjectDisposedException) { }
        }

        // The stop's handshake with what this controller has under way — taking office, the internal topics, a
        // request it is answering: none of it writes to the metadata log from here on, and what it was writing is
        // written when this returns.
        await _decisions.CloseAsync().ConfigureAwait(false);

        if (_controllerTask != null)
        {
            try { await _controllerTask; } catch (OperationCanceledException) { }
        }

        if (_raftNode != null)
        {
            // Detach what this controller attached, and nothing else. The node is the HOST's:
            // SurgewaveRuntime and Broker.App each create it, hand it to this controller AND to
            // the replication server, and dispose it themselves. Tearing it down from here ended
            // the metadata log and the election loops while the replication server still held it
            // and was still answering peers' RequestVote and AppendEntries with it.
            //
            // The subscription, on the other hand, is this controller's own and was never
            // released: the node keeps a strong reference to a disposed controller through it,
            // and in Broker.App — where the node is a DI singleton outliving everything — a
            // quorum loss would still be handled by a controller that had already shut down.
            _raftNode.OnQuorumLost -= HandleRaftQuorumLost;
            _raftNode.LeadershipChanged -= HandleRaftLeadershipChanged;
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
                // Never re-add the LOCAL broker from the cluster-nodes list (configs routinely list
                // all brokers including self): StartAsync just registered it with its authoritative
                // ports and self-advertised inter-broker protocol level, and the overwriting
                // AddBroker here would reset both — pinning the finalized level to KafkaWire
                // forever (#60 Inc5).
                if (brokerId == _config.BrokerId)
                    continue;

                // Optional 4th part: replication port (defaults to port + 1000 if not specified)
                int? replicationPort = null;
                if (parts.Length >= 4 && int.TryParse(parts[3], out var replPort))
                {
                    replicationPort = replPort;
                }

                var broker = replicationPort.HasValue
                    ? new BrokerNode { BrokerId = brokerId, Host = parts[1], Port = port, ReplicationPort = replicationPort.Value }
                    : new BrokerNode { BrokerId = brokerId, Host = parts[1], Port = port };

                // Insert-only (#72 Inc3): a peer that REGISTERED before config discovery ran carries
                // its advertised inter-broker protocol level and real replication port — the config
                // entry (level always KafkaWire, port possibly a derived guess) must seed unknown
                // peers, never clobber a registered one back down (startup-ordering race).
                _clusterState.UpdateBroker(brokerId, ifAbsent: broker, mutate: known => known, out var inserted);
                if (inserted)
                    LogDiscoveredBroker(brokerId, parts[1], port);
            }
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
    public async Task<bool> CreateTopicAsync(
        string topic,
        int partitionCount,
        short replicationFactor,
        Dictionary<string, string>? config,
        IReadOnlyList<IReadOnlyList<int>>? replicaAssignment,
        CancellationToken ct)
    {
        if (!_isController)
        {
            // Only the controller writes the topic to the metadata log; any other broker asks it to (#207). It used to
            // be refused here, and the handlers then created the topic on their own broker alone.
            if (_topicCreationRequester is null)
            {
                LogNotController("CreateTopic");
                return false;
            }
            return await _topicCreationRequester.RequestCreateTopicAsync(topic, partitionCount, replicationFactor, config, replicaAssignment, ct);
        }

        return await CreateTopicViaRaftAsync(topic, partitionCount, replicationFactor, config, replicaAssignment, ct);
    }

    /// <summary>
    /// Create topic via Raft consensus - proposes to Raft log and waits for commit.
    /// </summary>
    /// <remarks>
    /// One entry carries the topic and the replicas of every partition (#217): it commits whole, or a controller that
    /// lost its leadership on the way leaves no trace of the topic, and the client's retry creates it.
    /// </remarks>
    private async Task<bool> CreateTopicViaRaftAsync(
        string topic,
        int partitionCount,
        short replicationFactor,
        Dictionary<string, string>? config,
        IReadOnlyList<IReadOnlyList<int>>? replicaAssignment,
        CancellationToken ct)
    {
        // A topic the metadata log has already is not created again under a new id: two brokers creating the same
        // internal topic at once both find it there (#214).
        if (_clusterState.GetTopic(topic) is not null)
            return true;

        var brokerIds = _clusterState.Brokers.Keys.OrderBy(id => id).ToList();
        if (brokerIds.Count == 0)
        {
            LogNoBrokersAvailable(topic);
            return false;
        }
        // The client's own placement as it is, in its order — its first replica is the preferred leader (#202); the
        // broker that took the request checked it. Without one the controller places the replicas.
        var assignments = Enumerable.Range(0, partitionCount)
            .Select(p => new PartitionAssignment(
                p,
                replicaAssignment is not null ? [.. replicaAssignment[p]] : _replicaAssignment.AssignReplicas(brokerIds, p, replicationFactor),
                _config.MinInSyncReplicas))
            .ToList();

        var topicId = Guid.NewGuid();
        var command = new TopicCreatedCommand(topic, topicId, partitionCount, replicationFactor, config, assignments);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.TopicCreatedCommand);

        var index = await ProposeAsync(MetadataCommandType.TopicCreated, data, ct);
        if (index < 0)
        {
            LogNotController("CreateTopic (Raft propose failed)");
            return false;
        }

        // Wait for commit
        var committed = await _raftNode!.WaitForCommitAsync(index, TimeSpan.FromSeconds(10), ct);
        if (!committed)
        {
            LogRaftCommitTimeout("TopicCreated", index);
            return false;
        }

        await TakePartInAsync(topic, topicId, [.. assignments.Select(a => (a.Partition, a.Replicas))], ct);
        LogTopicCreated(topic, partitionCount, replicationFactor);
        return true;
    }

    /// <summary>
    /// Proposes the replica assignment of each partition and, once they commit, takes this broker's part in them.
    /// False when an assignment does not commit — the controller lost its leadership on the way, say — so the caller
    /// never reports a topic as created while some of its partitions have no replicas anywhere but here.
    /// </summary>
    /// <remarks>
    /// All assignments are proposed before the one wait for the last of them: the log commits in order, so the last
    /// committing means all did. Waiting for each in turn cost a replication round per partition, and the 50
    /// partitions of <c>__consumer_offsets</c> kept the controller busy for seconds (#204).
    /// </remarks>
    private async Task<bool> AssignPartitionsViaRaftAsync(
        string topic, Guid topicId, IEnumerable<int> partitions, short replicationFactor, CancellationToken ct)
    {
        var brokerIds = _clusterState.Brokers.Keys.OrderBy(id => id).ToList();
        if (brokerIds.Count == 0)
        {
            LogNoBrokersAvailable(topic);
            return false;
        }

        var assigned = new List<(int Partition, List<int> Replicas)>();
        long lastIndex = -1;
        foreach (var partition in partitions)
        {
            var replicas = _replicaAssignment.AssignReplicas(brokerIds, partition, replicationFactor);
            var assignCmd = new PartitionAssignedCommand(topic, partition, replicas, _config.MinInSyncReplicas);
            var assignData = JsonSerializer.SerializeToUtf8Bytes(assignCmd, ClusteringJsonContext.Default.PartitionAssignedCommand);

            lastIndex = await ProposeAsync(MetadataCommandType.PartitionAssigned, assignData, ct);
            if (lastIndex < 0)
            {
                LogNotController("CreateTopic (Raft propose failed)");
                return false;
            }
            assigned.Add((partition, replicas));
        }

        if (assigned.Count == 0)
            return true;
        if (!await _raftNode!.WaitForCommitAsync(lastIndex, TimeSpan.FromSeconds(5), ct))
        {
            LogRaftCommitTimeout("PartitionAssigned", lastIndex);
            return false;
        }

        await TakePartInAsync(topic, topicId, assigned, ct);
        return true;
    }

    // This broker's part in committed assignments: leading or following the partitions it is a replica of.
    private async Task TakePartInAsync(string topic, Guid topicId, List<(int Partition, List<int> Replicas)> assigned, CancellationToken ct)
    {
        foreach (var (partition, replicas) in assigned)
        {
            if (!replicas.Contains(_config.BrokerId))
                continue;
            var tp = new TopicPartition { Topic = topic, Partition = partition };
            if (replicas[0] == _config.BrokerId)
                await _replicaManager.BecomeLeaderAsync(tp, 1, ct, topicId);
            else
                await _replicaManager.BecomeFollowerAsync(tp, replicas[0], 1, ct, topicId);
        }
    }

    #region Raft-based Metadata Operations

    /// <summary>
    /// Register a broker via Raft consensus.
    /// </summary>
    public async Task<bool> RegisterBrokerViaRaftAsync(
        int brokerId, string host, int port, string? rack,
        Guid incarnationId, short interBrokerProtocol, int replicationPort, CancellationToken ct)
    {
        if (_raftNode == null || !_raftNode.IsLeader)
        {
            return false;
        }

        // #72 Inc5 — carry incarnation + level + replication port so the committed entry rebuilds the
        // membership store faithfully on replay; the broker epoch is the committed log index (assigned
        // in MetadataStateMachine.ApplyBrokerRegistered), not carried in the command.
        var command = new BrokerRegisteredCommand(
            brokerId, host, port, rack, incarnationId, interBrokerProtocol, replicationPort);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.BrokerRegisteredCommand);

        var index = await ProposeAsync(MetadataCommandType.BrokerRegistered, data, ct);
        if (index < 0) return false;

        var committed = await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct);
        if (committed)
        {
            LogBrokerRegisteredViaRaft(brokerId, host, port);

            // One more broker may be what the group coordinator topic was waiting for (#204).
            _ = EnsureInternalTopicsAsync(ct);
        }
        return committed;
    }

    /// <summary>
    /// Remove a broker via Raft consensus.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The guards below went in while this method still had no callers (#129), which is
    /// the cheap moment: they exist before an admin surface or KIP-853 voter removal
    /// reaches them, rather than being retrofitted after the first accident.
    /// </para>
    /// <para>
    /// Kafka added the same two when it closed this gap in KIP-1312, having had
    /// registration for years without an unregister. They are not symmetrical
    /// conveniences: refusing self-removal keeps the controller from dismantling the
    /// identity it is using to replicate the entry, and refusing an unknown id keeps a
    /// typo from committing an entry that does nothing while reporting success.
    /// </para>
    /// </remarks>
    public async Task<BrokerRemovalOutcome> RemoveBrokerViaRaftAsync(int brokerId, CancellationToken ct)
    {
        if (_raftNode == null || !_raftNode.IsLeader)
        {
            return BrokerRemovalOutcome.NotController;
        }

        // Being the Raft leader IS being the active controller, so reaching here with our
        // own id is exactly the case Kafka names.
        if (brokerId == _config.BrokerId)
        {
            LogRemoveSelfRefused(brokerId);
            return BrokerRemovalOutcome.CannotRemoveSelf;
        }

        if (_clusterState.GetBroker(brokerId) is null)
        {
            LogRemoveUnknownRefused(brokerId);
            return BrokerRemovalOutcome.UnknownBroker;
        }

        var command = new BrokerRemovedCommand(brokerId);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.BrokerRemovedCommand);

        var index = await ProposeAsync(MetadataCommandType.BrokerRemoved, data, ct);
        if (index < 0) return BrokerRemovalOutcome.NotCommitted;

        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct)
            ? BrokerRemovalOutcome.Removed
            : BrokerRemovalOutcome.NotCommitted;
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

        var index = await ProposeAsync(MetadataCommandType.IsrChanged, data, ct);
        if (index < 0) return false;

        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct);
    }

    /// <summary>
    /// Elect a new leader via Raft consensus.
    /// </summary>
    /// <summary>
    /// Commits a leadership decision to the metadata log. Returns the index of the committed
    /// entry, or -1 if it could not be proposed or did not commit in time.
    /// </summary>
    /// <remarks>
    /// The index is the caller's handle on WHERE the decision sits in the log, which a caller
    /// that is about to leave needs in order to check that its successor actually received it
    /// (#177). A plain "it committed" cannot answer that on a single-voter quorum.
    /// </remarks>
    private async Task<long> ElectLeaderViaRaftAsync(TopicPartition tp, int newLeader, int leaderEpoch, CancellationToken ct)
    {
        if (_raftNode == null || !_raftNode.IsLeader)
        {
            return -1;
        }

        var command = new LeaderChangedCommand(tp.Topic, tp.Partition, newLeader, leaderEpoch);
        var data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.LeaderChangedCommand);

        var index = await ProposeAsync(MetadataCommandType.LeaderChanged, data, ct);
        if (index < 0) return -1;

        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct) ? index : -1;
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

        var index = await ProposeAsync(MetadataCommandType.TopicDeleted, data, ct);
        if (index < 0) return false;

        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct);
    }

    /// <inheritdoc />
    public async Task<bool> SetGroupConfigAsync(string groupId, string name, string? value, CancellationToken ct)
    {
        if (!_isController)
        {
            // Only the controller writes to the metadata log; any other broker asks it to.
            if (_groupConfigRequester is null)
            {
                LogNotController("SetGroupConfig");
                return false;
            }
            return await _groupConfigRequester.RequestSetGroupConfigAsync(groupId, name, value, ct).ConfigureAwait(false);
        }

        if (_raftNode == null || !_raftNode.IsLeader)
            return false;
        var data = JsonSerializer.SerializeToUtf8Bytes(
            new GroupConfigChangedCommand(groupId, name, value), ClusteringJsonContext.Default.GroupConfigChangedCommand);
        var index = await ProposeAsync(MetadataCommandType.GroupConfigChanged, data, ct).ConfigureAwait(false);
        if (index < 0) return false;
        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> ChangeTopicConfigAsync(
        string topic, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed, CancellationToken ct)
    {
        if (!_isController)
        {
            // Only the controller writes to the metadata log; any other broker asks it to (#119). A broker changed its
            // own copy alone, and the topic's other replicas and every later leader ran it with the old config.
            if (_topicConfigRequester is null)
            {
                LogNotController("ChangeTopicConfig");
                return false;
            }
            return await _topicConfigRequester.RequestChangeTopicConfigAsync(topic, set, removed, ct).ConfigureAwait(false);
        }

        if (_raftNode == null || !_raftNode.IsLeader)
            return false;
        var data = JsonSerializer.SerializeToUtf8Bytes(
            new ConfigChangedCommand(topic, new Dictionary<string, string>(set), [.. removed]),
            ClusteringJsonContext.Default.ConfigChangedCommand);
        var index = await ProposeAsync(MetadataCommandType.ConfigChanged, data, ct).ConfigureAwait(false);
        if (index < 0) return false;
        return await _raftNode.WaitForCommitAsync(index, TimeSpan.FromSeconds(5), ct).ConfigureAwait(false);
    }

    #endregion

    /// <summary>
    /// Elect a new leader for a partition.
    /// </summary>
    public async Task<bool> ElectLeaderAsync(TopicPartition tp, int? preferredLeader = null, CancellationToken ct = default)
        => await ElectLeaderAtAsync(tp, preferredLeader, ct).ConfigureAwait(false) is not null;

    /// <summary>
    /// As <see cref="ElectLeaderAsync"/>, additionally reporting WHO was elected and WHERE the
    /// decision sits in the metadata log — what a caller needs to check that the new leader
    /// actually received it before this node walks away (#177).
    /// </summary>
    private async Task<(int NewLeader, long Index)?> ElectLeaderAtAsync(
        TopicPartition tp, int? preferredLeader, CancellationToken ct)
    {
        if (!_isController)
        {
            LogNotController("ElectLeader");
            return null;
        }

        var partitionState = _clusterState.GetPartitionState(tp);
        if (partitionState == null)
        {
            LogPartitionNotFound(tp.Topic, tp.Partition);
            return null;
        }

        var oldLeader = partitionState.LeaderBrokerId;
        var newLeaderEpoch = partitionState.LeaderEpoch + 1;

        // Determine new leader
        int newLeader;
        if (preferredLeader.HasValue && partitionState.Isr.Contains(preferredLeader.Value)
            && !HasOfflineLogDir(preferredLeader.Value))
        {
            newLeader = preferredLeader.Value;
        }
        else if (partitionState.Isr.FirstOrDefault(IsEligibleLeader, -1) is var liveIsrMember
                 && liveIsrMember >= 0)
        {
            // The first ISR member that is alive and whose log directory is online. A broker just declared dead can
            // still be in this view of the ISR — its removal is a committed log entry that may not be applied here yet
            // (#124); a broker with a failed log directory likewise (#162).
            newLeader = liveIsrMember;
        }
        else if (partitionState.Replicas.Count > 0)
        {
            // Unclean election from any alive replica — never one whose log directory is offline (#162), which would
            // be a leader that serves nothing.
            var aliveReplica = partitionState.Replicas.FirstOrDefault(IsEligibleLeader, -1);

            if (aliveReplica < 0)
            {
                LogNoReplicasAvailable(tp.Topic, tp.Partition);
                return null;
            }

            newLeader = aliveReplica;
            LogUncleanLeaderElection(tp.Topic, tp.Partition, newLeader);
        }
        else
        {
            LogNoReplicasAvailable(tp.Topic, tp.Partition);
            return null;
        }

        // The decision is a committed log entry that every broker applies; the apply drives
        // the replication transition as well as the state write (#165). There is no second
        // path to keep in step with it.
        var index = await ElectLeaderViaRaftAsync(tp, newLeader, newLeaderEpoch, ct);
        if (index < 0)
        {
            LogRaftCommitTimeout("LeaderChanged", 0);
            return null;
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

        LogLeaderElected(tp.Topic, tp.Partition, oldLeader, newLeader, newLeaderEpoch);
        return (newLeader, index);
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

        // A replica in a failed log directory does not rejoin an ISR while the directory is offline (#162): the leader
        // may still count it until its lag expires, and its report would otherwise put it back. Kafka refuses such an
        // AlterPartition as INELIGIBLE_REPLICA; here the ineligible replica is left out of what is committed.
        var isr = newIsr.Where(b => !HasOfflineLogDir(b)).ToList();
        if (isr.Count == 0)
            isr = [.. newIsr];

        // Apply the ISR wholesale (an ISR-only change does not bump LeaderEpoch). Replicas
        // converge by applying the committed entry, not by being told.
        await UpdateIsrViaRaftAsync(tp, isr, ct);

        var updated = _clusterState.GetPartitionState(tp);

        LogIsrUpdateApplied(tp.Topic, tp.Partition, leaderId, string.Join(",", newIsr));
        return updated;
    }

    /// <summary>
    /// Handle broker failure detected by heartbeat manager.
    /// </summary>
    /// <summary>
    /// Controller-side controlled shutdown (#180): move every leadership off a departing broker.
    /// </summary>
    /// <remarks>
    /// The same work the failure path does, minus the assumption that anything went wrong — the
    /// broker is leaving on purpose and is still reachable, so this runs while it can still serve,
    /// instead of after a heartbeat timeout has already made its partitions unavailable.
    /// </remarks>
    public async Task<IReadOnlyList<TopicPartition>> MoveLeadershipAwayAsync(
        int brokerId, CancellationToken ct = default)
    {
        if (!_isController)
        {
            LogNotController("ControlledShutdown");
            return [.. _clusterState.PartitionStates
                .Where(p => p.Value.LeaderBrokerId == brokerId)
                .Select(p => p.Key)];
        }

        LogControlledShutdownRequested(brokerId);

        var stillLed = new List<TopicPartition>();

        foreach (var (tp, state) in _clusterState.PartitionStates)
        {
            if (ct.IsCancellationRequested)
                break;

            if (state.LeaderBrokerId != brokerId)
            {
                // Not leading it, but it may be in the ISR — a departing broker must not keep
                // holding up acks=all for the replicas that stay. Through the log, so every broker
                // drops it, not only this controller (#124).
                if (state.Isr.Contains(brokerId) && state.Isr.Count > 1)
                    await UpdateIsrViaRaftAsync(tp, [.. state.Isr.Where(b => b != brokerId)], ct).ConfigureAwait(false);

                continue;
            }

            var successor = state.Isr.FirstOrDefault(b => b != brokerId, -1);
            if (successor < 0)
            {
                // No in-sync replica but this one. Moving it anyway would be an unclean election,
                // which is not this path's call to make.
                stillLed.Add(tp);
                LogNoEligibleLeaderForPartition(tp.Topic, tp.Partition);
                continue;
            }

            if (await ElectLeaderAsync(tp, successor, ct).ConfigureAwait(false))
            {
                LogPartitionLeadershipTransferred(tp.Topic, tp.Partition, successor);
            }
            else
            {
                stillLed.Add(tp);
                LogLeadershipHandoverFailed(tp.Topic, tp.Partition, successor);
            }
        }

        LogControlledShutdownCompleted(brokerId, stillLed.Count);
        return stillLed;
    }

    /// <summary>
    /// Controller-side handling of a failed log directory (#162): every partition the broker replicates is moved off
    /// it. Where it leads, an eligible in-sync replica is elected; where it is in the ISR, it leaves it. Returns the
    /// partitions it still leads because no other in-sync replica can take them.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Kafka's controller does the same with the OfflineLogDirs of a heartbeat (<c>handleDirectoriesOffline</c>): the
    /// replicas in those directories leave the ISRs and lose their leaderships. Surgewave keeps all of a broker's
    /// replicas in its one log directory, so a broker reporting it offline has all of them offline.
    /// </para>
    /// <para>
    /// A partition no other in-sync replica can lead stays with the broker, whose storage answers it with
    /// KAFKA_STORAGE_ERROR. Electing an out-of-sync replica would be an unclean election, and this path does not make
    /// it, any more than a controlled shutdown does (#180). The broker also stays in that partition's ISR: an ISR
    /// without its leader would be one no election can choose from.
    /// </para>
    /// <para>
    /// Idempotent: a partition already moved finds the broker neither leading it nor in its ISR. That is what lets a
    /// controller elected later redo it from the next heartbeat, which repeats the offline directories.
    /// </para>
    /// </remarks>
    public async Task<IReadOnlyList<TopicPartition>> MoveReplicasOffOfflineLogDirAsync(int brokerId, CancellationToken ct = default)
    {
        if (!_isController)
        {
            LogNotController("HandleLogDirsOffline");
            return [];
        }

        LogMovingOffOfflineLogDir(brokerId);

        var stillLed = new List<TopicPartition>();
        foreach (var (tp, state) in _clusterState.PartitionStates)
        {
            ct.ThrowIfCancellationRequested();

            if (!state.Replicas.Contains(brokerId))
                continue;

            var isr = _clusterState.GetIsrSnapshot(tp);
            var remainingIsr = isr.Where(b => b != brokerId).ToList();

            if (state.LeaderBrokerId == brokerId)
            {
                var successor = remainingIsr.FirstOrDefault(IsEligibleLeader, -1);
                if (successor < 0)
                {
                    stillLed.Add(tp);
                    LogNoEligibleLeaderForPartition(tp.Topic, tp.Partition);
                    continue;
                }

                if (!await ElectLeaderAsync(tp, successor, ct).ConfigureAwait(false))
                {
                    stillLed.Add(tp);
                    LogLeadershipHandoverFailed(tp.Topic, tp.Partition, successor);
                    continue;
                }

                LogPartitionLeadershipTransferred(tp.Topic, tp.Partition, successor);
            }

            if (remainingIsr.Count != isr.Count && remainingIsr.Count > 0
                && await UpdateIsrViaRaftAsync(tp, remainingIsr, ct).ConfigureAwait(false))
            {
                LogRemovedFromIsr(tp.Topic, tp.Partition, brokerId);
            }
        }

        LogMovedOffOfflineLogDir(brokerId, stillLed.Count);
        return stillLed;
    }

    private async Task HandleLogDirsOfflineAsync(int brokerId)
    {
        // For as long as this controller runs, as the failure path does (#234).
        var ct = _lifetime;
        try
        {
            await MoveReplicasOffOfflineLogDirAsync(brokerId, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // This controller stops; the next one learns of the directory from the broker's next heartbeat.
        }
        catch (Exception ex)
        {
            LogOfflineLogDirHandlingFailed(ex, brokerId);
        }
    }

    private async Task HandleBrokerFailedAsync(int failedBrokerId)
    {
        LogBrokerFailureDetected(failedBrokerId);

        // A controller that dies is replaced by a Raft election, which the leader watch
        // notices; there is nothing for a follower to do about it here.
        if (!_isController)
            return;

        // As controller, handle the broker failure: per partition, drop the dead broker from the ISR and,
        // where it led, elect a successor from what remains. Both through the metadata log (#124): the ISR
        // removal used to be a write to this controller's own ClusterState only, so every other broker kept
        // reporting the dead one in-sync, and a controller elected later rebuilt the ISR with it from the log.
        //
        // For as long as this controller runs (#234): each decision waits for its commit, which a controller that
        // lost its quorum never gets, and the failure of a broker with a hundred partitions held a stopping broker
        // for minutes — the heartbeat monitor awaits this handler, and the broker's shutdown awaits the monitor.
        //
        // The lifetime, not the source it came from: the stop clears that field, and a failure raised after it —
        // the monitor is stopped after this controller — found none and ran with no end at all.
        var ct = _lifetime;
        var affectedPartitions = new List<TopicPartition>();

        try
        {
            foreach (var (tp, state) in _clusterState.PartitionStates)
            {
                ct.ThrowIfCancellationRequested();

                var remainingIsr = state.Isr.Where(b => b != failedBrokerId).ToList();
                if (remainingIsr.Count != state.Isr.Count)
                {
                    if (await UpdateIsrViaRaftAsync(tp, remainingIsr, ct).ConfigureAwait(false))
                    {
                        affectedPartitions.Add(tp);
                        LogRemovedFromIsr(tp.Topic, tp.Partition, failedBrokerId);
                    }
                }

                if (state.LeaderBrokerId == failedBrokerId)
                {
                    LogLeaderFailedForPartition(tp.Topic, tp.Partition, failedBrokerId);
                    // The successor named from the ISR just computed, not read back: the committed ISR change
                    // may not be applied here yet, and the read would still offer the dead broker.
                    await ElectLeaderAsync(tp, remainingIsr.Count > 0 ? remainingIsr[0] : null, ct)
                        .ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // This controller stops; the next one handles the failure from the metadata log.
            return;
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
            // Skip if the preferred leader is not alive, or its log directory is offline (#162)
            if (!IsEligibleLeader(state.PreferredLeader))
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

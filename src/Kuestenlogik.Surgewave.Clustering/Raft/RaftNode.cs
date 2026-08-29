using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Core Raft consensus node implementing the Raft protocol.
/// Handles leader election, log replication, and state machine application.
/// </summary>
public sealed partial class RaftNode : IAsyncDisposable
{
    private readonly ILogger<RaftNode> _logger;
    private readonly ClusteringConfig _config;
    private readonly RaftPersistence _persistence;
    private readonly IRaftTransport _transport;
    private readonly IRaftStateMachine _stateMachine;
    private readonly IRaftVoterSet _voterSet;
    private readonly object _lock = new();
    private readonly Random _random = new();

    // Persistent state (persisted to disk before responding to RPCs)
    private int _currentTerm;
    private int? _votedFor;
    private readonly List<RaftLogEntry> _log = [];

    // Volatile state on all servers
    private long _commitIndex;
    private long _lastApplied;
    private RaftState _state = RaftState.Follower;
    private int? _leaderId;

    // Volatile state on leaders (reinitialized after election)
    private readonly ConcurrentDictionary<int, long> _nextIndex = new();
    private readonly ConcurrentDictionary<int, long> _matchIndex = new();
    private readonly ConcurrentDictionary<int, DateTimeOffset> _lastPeerContact = new();

    // Timing
    private DateTimeOffset _lastHeartbeat = DateTimeOffset.UtcNow;
    private int _electionTimeoutMs;

    // Shutdown state
    private volatile bool _isShuttingDown;

    /// <summary>
    /// Event raised when the leader detects it has lost quorum connectivity.
    /// </summary>
    public event EventHandler? OnQuorumLost;

    private CancellationTokenSource? _cts;
    private Task? _electionTask;
    private Task? _heartbeatTask;

    public int NodeId => _config.BrokerId;
    public int CurrentTerm => _currentTerm;
    public RaftState State => _state;
    public int? LeaderId => _leaderId;
    public bool IsLeader => _state == RaftState.Leader;
    public long CommitIndex => _commitIndex;
    public long LastLogIndex => _log.Count > 0 ? _log[^1].Index : 0;
    public int LastLogTerm => _log.Count > 0 ? _log[^1].Term : 0;

    /// <summary>
    /// Get the match index for a peer (for DescribeQuorum API).
    /// Returns the last known replicated log index for the peer.
    /// </summary>
    public long GetPeerMatchIndex(int peerId)
    {
        return _matchIndex.TryGetValue(peerId, out var index) ? index : 0;
    }

    /// <summary>
    /// Get the last contact time for a peer (for DescribeQuorum API).
    /// Returns null if never contacted.
    /// </summary>
    public DateTimeOffset? GetPeerLastContact(int peerId)
    {
        return _lastPeerContact.TryGetValue(peerId, out var time) ? time : null;
    }

    /// <summary>
    /// Get all peer IDs known to this node.
    /// </summary>
    public IReadOnlyList<int> GetPeerIds()
    {
        return PeerIds;
    }

    /// <summary>
    /// Whether this node votes, or only follows the log (#167).
    /// </summary>
    /// <remarks>
    /// Read by <c>DescribeQuorum</c>, which has to put this node in the right list: reporting
    /// an observer among the voters overstates how many nodes must agree, and that number is
    /// what the API exists to answer.
    /// </remarks>
    public bool IsVoterNode => IsVoter;

    /// <summary>
    /// Returns true if the leader has contact with a majority of peers within the isolation timeout.
    /// </summary>
    public bool HasQuorumConnectivity
    {
        get
        {
            if (_state != RaftState.Leader)
                return false;

            var peers = PeerIds;
            if (peers.Count == 0)
                return true; // Single node cluster

            var majority = _voterSet.Majority;
            var isolationTimeout = TimeSpan.FromMilliseconds(_config.RaftIsolationTimeoutMs);
            var now = DateTimeOffset.UtcNow;

            // Count reachable peers (peers contacted within the isolation timeout)
            var reachablePeers = peers.Count(peerId =>
                _lastPeerContact.TryGetValue(peerId, out var lastContact) &&
                (now - lastContact) < isolationTimeout);

            // Include self in the count
            var reachableNodes = reachablePeers + 1;
            return reachableNodes >= majority;
        }
    }

    public RaftNode(
        ILogger<RaftNode> logger,
        ClusteringConfig config,
        RaftPersistence persistence,
        IRaftTransport transport,
        IRaftStateMachine stateMachine,
        IRaftVoterSet? voterSet = null)
    {
        _logger = logger;
        _config = config;
        _persistence = persistence;
        _transport = transport;
        _stateMachine = stateMachine;

        // Who votes is a membership question, not a transport one (#166). Defaults to the
        // set Surgewave has today — every broker the transport knows — so this changes
        // nothing until an explicit controller quorum supplies a different one.
        _voterSet = voterSet ?? new TransportDerivedVoterSet(transport, config.BrokerId);

        ResetElectionTimeout();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Load persistent state
        var state = await _persistence.LoadStateAsync(cancellationToken);
        _currentTerm = state.CurrentTerm;
        _votedFor = state.VotedFor;

        // Load log entries
        var entries = await _persistence.LoadLogAsync(cancellationToken);
        _log.AddRange(entries);

        // Wait for at least one peer to be reachable before enabling elections
        // This prevents split-brain during sequential broker startup
        await WaitForPeerReadinessAsync(_cts.Token);

        // Start election timeout loop
        _electionTask = Task.Run(() => ElectionTimeoutLoopAsync(_cts.Token), _cts.Token);

        LogRaftNodeStarted(NodeId, _currentTerm, _log.Count);
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is null) return;

        try { await _cts.CancelAsync(); }
        catch (ObjectDisposedException) { return; }

        if (_electionTask != null)
        {
            try { await _electionTask; } catch (OperationCanceledException) { }
        }

        if (_heartbeatTask != null)
        {
            try { await _heartbeatTask; } catch (OperationCanceledException) { }
        }

        _cts.Dispose();
        _cts = null;
    }

    /// <summary>
    /// Handle PreVote RPC from a candidate (Pre-Vote protocol extension).
    ///
    /// Pre-Vote is similar to RequestVote but:
    /// 1. Does not increment the candidate's term
    /// 2. Does not cause the receiver to update its term or become a follower
    /// 3. Helps prevent disruptive elections from partitioned nodes
    ///
    /// A pre-vote is granted if:
    /// - The proposed term is >= our current term
    /// - The candidate's log is at least as up-to-date as ours
    /// - We haven't heard from a leader recently (optional, prevents leader disruption)
    /// </summary>
    public Task<PreVoteResponse> HandlePreVoteAsync(PreVoteRequest request, CancellationToken ct)
    {
        PreVoteResponse response;

        lock (_lock)
        {
            // An observer is not part of the quorum, so its answer must not be counted
            // towards one (#167). A candidate does not ask it; refusing is the guard for a
            // stale voter list on the other side, where granting would let a minority of
            // real voters plus observers elect a leader.
            if (!IsVoter)
            {
                response = new PreVoteResponse(_currentTerm, false);
                return Task.FromResult(response);
            }

            // If proposed term is less than our current term, reject
            if (request.ProposedTerm < _currentTerm)
            {
                response = new PreVoteResponse(_currentTerm, false);
                return Task.FromResult(response);
            }

            // Check if we've heard from a leader recently
            // If so, reject to prevent disrupting a working cluster
            var timeSinceLastHeartbeat = (DateTimeOffset.UtcNow - _lastHeartbeat).TotalMilliseconds;
            if (_leaderId.HasValue && timeSinceLastHeartbeat < _config.RaftElectionTimeoutMinMs)
            {
                LogPreVoteRejectedLeaderActive(request.CandidateId, _leaderId.Value);
                response = new PreVoteResponse(_currentTerm, false);
                return Task.FromResult(response);
            }

            // Grant pre-vote if candidate's log is at least as up-to-date as ours
            var canVote = IsLogUpToDate(request.LastLogIndex, request.LastLogTerm);

            if (canVote)
            {
                LogPreVoteGranted(request.CandidateId, request.ProposedTerm);
            }

            response = new PreVoteResponse(_currentTerm, canVote);
        }

        // Note: Pre-Vote does NOT persist state changes (no term update, no votedFor)
        return Task.FromResult(response);
    }

    /// <summary>
    /// Handle RequestVote RPC from a candidate.
    /// </summary>
    public async Task<RequestVoteResponse> HandleRequestVoteAsync(RequestVoteRequest request, CancellationToken ct)
    {
        bool stateChanged;
        int termToSave;
        int? votedForToSave;
        RequestVoteResponse response;

        lock (_lock)
        {
            // If term is stale, reject
            if (request.Term < _currentTerm)
            {
                return new RequestVoteResponse(_currentTerm, false);
            }

            stateChanged = false;

            // If we see a newer term, become follower
            if (request.Term > _currentTerm)
            {
                BecomeFollower(request.Term);
                stateChanged = true;
            }

            // Grant vote if:
            // 1. We haven't voted for anyone else in this term
            // 2. Candidate's log is at least as up-to-date as ours
            // 3. We are a voter — an observer is outside the quorum, so counting its answer
            //    would let a minority of real voters elect a leader (#167). The term above is
            //    still adopted: an observer that has seen a newer term must not keep serving
            //    the old one, it simply does not get a say in who wins it.
            var canVote = IsVoter &&
                          (_votedFor == null || _votedFor == request.CandidateId) &&
                          IsLogUpToDate(request.LastLogIndex, request.LastLogTerm);

            if (canVote)
            {
                _votedFor = request.CandidateId;
                _lastHeartbeat = DateTimeOffset.UtcNow;
                stateChanged = true;
                LogVotedFor(request.CandidateId, _currentTerm);
            }

            // Capture state for persistence
            termToSave = _currentTerm;
            votedForToSave = _votedFor;
            response = new RequestVoteResponse(_currentTerm, canVote);
        }

        // Persist state before responding (Raft correctness requirement)
        if (stateChanged)
        {
            await _persistence.SaveStateAsync(termToSave, votedForToSave, ct);
        }

        return response;
    }

    /// <summary>
    /// Handle AppendEntries RPC from leader.
    /// </summary>
    public async Task<AppendEntriesResponse> HandleAppendEntriesAsync(AppendEntriesRequest request, CancellationToken ct)
    {
        bool stateChanged;
        bool logChanged;
        bool logTruncated = false;
        long truncateFromIndex = 0;
        int termToSave;
        int? votedForToSave;
        List<RaftLogEntry> entriesToPersist = [];
        AppendEntriesResponse response;

        lock (_lock)
        {
            // If term is stale, reject
            if (request.Term < _currentTerm)
            {
                return new AppendEntriesResponse(_currentTerm, false, 0);
            }

            // Reset election timeout (we heard from the leader)
            _lastHeartbeat = DateTimeOffset.UtcNow;

            stateChanged = false;

            // If we see a newer term or we're candidate, become follower
            if (request.Term > _currentTerm || _state == RaftState.Candidate)
            {
                BecomeFollower(request.Term);
                stateChanged = true;
            }

            _leaderId = request.LeaderId;

            // Capture state for persistence
            termToSave = _currentTerm;
            votedForToSave = _votedFor;

            // Check if log contains entry at prevLogIndex with prevLogTerm
            if (request.PrevLogIndex > 0)
            {
                var prevEntry = GetLogEntry(request.PrevLogIndex);
                if (prevEntry == null || prevEntry.Term != request.PrevLogTerm)
                {
                    // Log doesn't contain matching entry
                    response = new AppendEntriesResponse(_currentTerm, false, 0);
                    logChanged = false;
                    goto PersistAndReturn;
                }
            }

            logChanged = false;

            // Append new entries (delete conflicting entries first)
            if (request.Entries.Length > 0)
            {
                foreach (var entry in request.Entries)
                {
                    var existing = GetLogEntry(entry.Index);
                    if (existing != null && existing.Term != entry.Term)
                    {
                        // Delete this entry and all following
                        TruncateLogFrom(entry.Index);
                        logTruncated = true;
                        truncateFromIndex = entry.Index;
                    }

                    if (GetLogEntry(entry.Index) == null)
                    {
                        _log.Add(entry);
                        entriesToPersist.Add(entry);
                        logChanged = true;
                    }
                }

                LogEntriesAppended(request.Entries.Length, LastLogIndex);
            }

            // Update commit index
            if (request.LeaderCommit > _commitIndex)
            {
                _commitIndex = Math.Min(request.LeaderCommit, LastLogIndex);
                ApplyCommittedEntries();
            }

            response = new AppendEntriesResponse(_currentTerm, true, LastLogIndex);
        }

    PersistAndReturn:
        // Persist state before responding (Raft correctness requirement)
        if (stateChanged)
        {
            await _persistence.SaveStateAsync(termToSave, votedForToSave, ct);
        }

        // Persist log changes
        if (logTruncated)
        {
            await _persistence.TruncateLogAsync(truncateFromIndex, ct);
        }
        if (logChanged && entriesToPersist.Count > 0)
        {
            await _persistence.AppendEntriesAsync(entriesToPersist, ct);
        }

        return response;
    }

    /// <summary>
    /// Propose a new command to be replicated (leader only).
    /// Returns the log index if successful, or -1 if not leader.
    /// </summary>
    public async Task<long> ProposeAsync(MetadataCommandType commandType, byte[] data, CancellationToken ct)
    {
        RaftLogEntry? entryToSave;
        long resultIndex;

        lock (_lock)
        {
            if (_state != RaftState.Leader)
            {
                return -1;
            }

            var entry = new RaftLogEntry
            {
                Term = _currentTerm,
                Index = LastLogIndex + 1,
                CommandType = commandType,
                Data = data,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            _log.Add(entry);
            entryToSave = entry;
            resultIndex = entry.Index;
            LogEntryProposed(entry.Index, commandType);
        }

        // Persist log entry before considering it proposed (Raft correctness requirement)
        await _persistence.AppendEntriesAsync([entryToSave], ct);

        // Will be replicated via heartbeat loop
        return resultIndex;
    }

    /// <summary>
    /// Wait for a log entry to be committed.
    /// </summary>
    public async Task<bool> WaitForCommitAsync(long index, TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;

        while (DateTimeOffset.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            if (_commitIndex >= index)
                return true;

            await Task.Delay(10, ct);
        }

        return _commitIndex >= index;
    }

    private async Task ElectionTimeoutLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(50, ct); // Check every 50ms

                // Don't start elections during graceful shutdown
                if (_isShuttingDown)
                    continue;

                if (_state == RaftState.Leader)
                    continue;

                // An observer has no vote, so it cannot win and its campaign would only
                // disrupt a healthy term (#167). It stays a follower for as long as it is
                // outside the quorum.
                if (!IsVoter)
                    continue;

                var elapsed = (DateTimeOffset.UtcNow - _lastHeartbeat).TotalMilliseconds;
                if (elapsed >= _electionTimeoutMs)
                {
                    await StartElectionAsync(ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogElectionLoopError(ex);
            }
        }
    }

    private async Task StartElectionAsync(CancellationToken ct)
    {
        // ========================================
        // Phase 1: Pre-Vote (Raft extension)
        // ========================================
        // Before incrementing term and starting a real election, check if we
        // would get enough votes. This prevents a partitioned node from
        // repeatedly incrementing its term and disrupting the cluster.

        var peers = PeerIds;
        var majority = _voterSet.Majority;
        var proposedTerm = _currentTerm + 1;

        // Split-brain prevention: Don't start election when peers are expected but not yet discovered.
        if (peers.Count == 0 && ExpectsClusterPeers())
        {
            LogWaitingForPeerDiscovery(_currentTerm);
            return;
        }

        LogStartingPreVote(proposedTerm);

        var preVoteRequest = new PreVoteRequest(
            proposedTerm,
            NodeId,
            LastLogIndex,
            LastLogTerm
        );

        var preVotes = 1; // Pre-vote for self
        var preVoteTasks = peers.Select(async peerId =>
        {
            try
            {
                var response = await _transport.SendPreVoteAsync(peerId, preVoteRequest, ct);
                return (peerId, response);
            }
            catch
            {
                return (peerId, (PreVoteResponse?)null);
            }
        });

        var preVoteResults = await Task.WhenAll(preVoteTasks);

        lock (_lock)
        {
            // Process pre-vote responses
            foreach (var (peerId, response) in preVoteResults)
            {
                if (response == null)
                    continue;

                // If we see a higher term, update but don't become follower yet
                // (pre-vote responses don't trigger term updates per se, but we track it)
                if (response.Term > _currentTerm)
                {
                    // Someone has a higher term, we should abort and become follower
                    BecomeFollower(response.Term);
                    return;
                }

                if (response.VoteGranted)
                {
                    preVotes++;
                    LogReceivedPreVote(peerId, preVotes, majority);
                }
            }

            // If we didn't get enough pre-votes, abort election
            if (preVotes < majority)
            {
                LogPreVoteFailed(preVotes, majority);
                // Stay as follower, will retry after election timeout
                return;
            }

            LogPreVoteSucceeded(preVotes, majority);
        }

        // ========================================
        // Phase 2: Real Election (standard Raft)
        // ========================================
        // We got enough pre-votes, now start the actual election

        int termToSave;
        int votedForToSave;

        lock (_lock)
        {
            _state = RaftState.Candidate;
            _currentTerm++;
            _votedFor = NodeId;
            _lastHeartbeat = DateTimeOffset.UtcNow;
            ResetElectionTimeout();

            termToSave = _currentTerm;
            votedForToSave = NodeId;
        }

        // Persist state before sending RequestVote RPCs (Raft correctness requirement)
        await _persistence.SaveStateAsync(termToSave, votedForToSave, ct);

        LogStartedElection(_currentTerm);

        var votes = 1; // Vote for self

        var request = new RequestVoteRequest(
            _currentTerm,
            NodeId,
            LastLogIndex,
            LastLogTerm
        );

        var tasks = peers.Select(async peerId =>
        {
            try
            {
                var response = await _transport.SendRequestVoteAsync(peerId, request, ct);
                return (peerId, response);
            }
            catch
            {
                return (peerId, (RequestVoteResponse?)null);
            }
        });

        var results = await Task.WhenAll(tasks);

        lock (_lock)
        {
            // Check if we're still a candidate
            if (_state != RaftState.Candidate || _currentTerm != request.Term)
                return;

            foreach (var (peerId, response) in results)
            {
                if (response == null)
                    continue;

                if (response.Term > _currentTerm)
                {
                    BecomeFollower(response.Term);
                    return;
                }

                if (response.VoteGranted)
                {
                    votes++;
                    LogReceivedVote(peerId, votes, majority);
                }
            }

            if (votes >= majority)
            {
                BecomeLeader();
            }
        }
    }

    /// <summary>
    /// Returns true if this node is configured to be part of a multi-node cluster.
    /// Used to prevent self-election when peers are expected but not yet discovered.
    /// </summary>
    private bool ExpectsClusterPeers()
    {
        // If ClusterNodes is configured, we expect peers
        return !string.IsNullOrWhiteSpace(_config.ClusterNodes);
    }

    /// <summary>
    /// Waits for at least one peer to be reachable before enabling elections.
    /// This prevents split-brain during sequential broker startup.
    /// </summary>
    private async Task WaitForPeerReadinessAsync(CancellationToken ct)
    {
        // Skip if single-node mode (no peers expected)
        if (!ExpectsClusterPeers())
            return;

        // Skip if timeout is disabled
        if (_config.RaftPeerDiscoveryTimeoutSeconds <= 0)
            return;

        var peers = PeerIds;
        if (peers.Count == 0)
        {
            LogWaitingForPeerDiscoveryStart();
        }

        var timeout = TimeSpan.FromSeconds(_config.RaftPeerDiscoveryTimeoutSeconds);
        var deadline = DateTimeOffset.UtcNow + timeout;

        while (DateTimeOffset.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            // Refresh in case new brokers were registered — the voter set is queried, not
            // snapshotted, precisely so this keeps working.
            peers = PeerIds;

            // Try to reach at least one peer
            foreach (var peerId in peers)
            {
                if (await _transport.IsPeerReachableAsync(peerId, ct))
                {
                    LogPeerDiscovered(peerId);
                    return;
                }
            }

            await Task.Delay(100, ct);
        }

        LogPeerDiscoveryTimeout(_config.RaftPeerDiscoveryTimeoutSeconds);
    }

    private void BecomeFollower(int term)
    {
        _state = RaftState.Follower;
        _currentTerm = term;
        _votedFor = null;
        _leaderId = null;

        // Cancel heartbeat task if running
        LogBecameFollower(term);
    }

    private void BecomeLeader()
    {
        _state = RaftState.Leader;
        _leaderId = NodeId;

        // Initialize leader state
        _nextIndex.Clear();
        _matchIndex.Clear();

        foreach (var peerId in PeerIds)
        {
            _nextIndex[peerId] = LastLogIndex + 1;
            _matchIndex[peerId] = 0;
        }

        // Observers are replicated to on the same terms as voters — they just do not count
        // (#167). Without this they would have no nextIndex and SendAppendEntriesAsync would
        // skip them, which is a broker with no metadata log at all.
        foreach (var observerId in ObserverIds)
        {
            _nextIndex[observerId] = LastLogIndex + 1;
            _matchIndex[observerId] = 0;
        }

        LogBecameLeader(_currentTerm);

        // Start heartbeat loop
        _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(_cts!.Token));

        // Append no-op entry to establish leadership
        _ = ProposeAsync(MetadataCommandType.Noop, [], CancellationToken.None);
    }

    private async Task HeartbeatLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && _state == RaftState.Leader)
        {
            try
            {
                await SendHeartbeatsAsync(ct);

                // Check for quorum connectivity (isolation detection)
                if (!HasQuorumConnectivity)
                {
                    LogQuorumLost();
                    await StepDownAsync();
                    break;
                }

                await Task.Delay(_config.RaftHeartbeatIntervalMs, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogHeartbeatLoopError(ex);
            }
        }
    }

    /// <summary>
    /// Gracefully steps down from leadership, becoming a follower.
    /// Used when detecting network isolation or on controlled shutdown.
    /// </summary>
    public Task StepDownAsync()
    {
        lock (_lock)
        {
            if (_state != RaftState.Leader)
                return Task.CompletedTask;

            LogSteppingDown(_currentTerm);
            BecomeFollower(_currentTerm);
        }

        // Notify listeners (e.g., ClusterController)
        try
        {
            OnQuorumLost?.Invoke(this, EventArgs.Empty);
        }
        catch (Exception ex)
        {
            LogQuorumLostHandlerError(ex);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Event raised when graceful shutdown is initiated.
    /// </summary>
    public event EventHandler? OnGracefulShutdown;

    /// <summary>
    /// Initiates a graceful shutdown of the Raft node.
    /// Steps down from leadership and allows time for a new leader to be elected.
    /// </summary>
    /// <param name="timeout">Maximum time to wait for leader election to complete</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>True if shutdown was graceful (new leader elected or was not leader)</returns>
    public async Task<bool> GracefulShutdownAsync(TimeSpan timeout, CancellationToken ct = default)
    {
        // Mark as shutting down to prevent new elections
        _isShuttingDown = true;

        var wasLeader = false;

        lock (_lock)
        {
            wasLeader = _state == RaftState.Leader;
        }

        if (!wasLeader)
        {
            LogGracefulShutdownNotLeader();
            return true;
        }

        LogGracefulShutdownInitiated(_currentTerm);

        // Step down from leadership
        await StepDownAsync();

        // Notify listeners about graceful shutdown
        try
        {
            OnGracefulShutdown?.Invoke(this, EventArgs.Empty);
        }
        catch (Exception ex)
        {
            LogGracefulShutdownHandlerError(ex);
        }

        // Wait for a new leader to be elected
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            // Check if a new leader has been elected (not us)
            if (_leaderId.HasValue && _leaderId.Value != NodeId)
            {
                LogGracefulShutdownNewLeader(_leaderId.Value);
                return true;
            }

            await Task.Delay(50, ct);
        }

        LogGracefulShutdownTimeout();
        return false;
    }

    private async Task SendHeartbeatsAsync(CancellationToken ct)
    {
        var peers = PeerIds;
        var observers = ObserverIds;

        // A node that joined mid-term has no replication state yet, because BecomeLeader ran
        // before it existed. Seeding it here rather than skipping it is what lets a broker
        // start into a running cluster and receive the log (#167).
        EnsureReplicationState(peers);
        EnsureReplicationState(observers);

        var tasks = peers.Concat(observers).Select(peerId => SendAppendEntriesAsync(peerId, ct));
        await Task.WhenAll(tasks);

        // Update commit index based on match indices
        lock (_lock)
        {
            if (_state != RaftState.Leader)
                return;

            // Find the highest index replicated on a majority — of the VOTERS (#167).
            // Counting observers here would let their acknowledgements stand in for a
            // quorum: three observers behind two voters would commit an entry that only one
            // voter holds, and losing that voter loses committed metadata.
            var matchIndices = new List<long>(peers.Count + 1) { LastLogIndex };
            foreach (var voterId in peers)
            {
                matchIndices.Add(_matchIndex.TryGetValue(voterId, out var match) ? match : 0);
            }

            matchIndices.Sort();
            matchIndices.Reverse();
            var majorityIndex = matchIndices.Count / 2;
            var newCommitIndex = matchIndices[majorityIndex];

            if (newCommitIndex > _commitIndex)
            {
                var entry = GetLogEntry(newCommitIndex);
                if (entry != null && entry.Term == _currentTerm)
                {
                    _commitIndex = newCommitIndex;
                    LogCommitIndexAdvanced(_commitIndex);
                    ApplyCommittedEntries();
                }
            }
        }
    }

    /// <summary>
    /// Gives nodes that appeared after this leader took office the replication state
    /// <see cref="SendAppendEntriesAsync"/> needs (#167).
    /// </summary>
    private void EnsureReplicationState(IReadOnlyList<int> nodeIds)
    {
        // The common case is that everything is already seeded, so this walks the list first
        // and takes the lock only when a node is actually missing. The lock is for
        // LastLogIndex, which reads the log; the index dictionaries are concurrent.
        var missing = false;
        foreach (var nodeId in nodeIds)
        {
            if (!_nextIndex.ContainsKey(nodeId)) { missing = true; break; }
        }

        if (!missing)
            return;

        lock (_lock)
        {
            var nextIndex = LastLogIndex + 1;
            foreach (var nodeId in nodeIds)
            {
                _nextIndex.TryAdd(nodeId, nextIndex);
                _matchIndex.TryAdd(nodeId, 0);
            }
        }
    }

    private async Task SendAppendEntriesAsync(int peerId, CancellationToken ct)
    {
        try
        {
            long prevLogIndex;
            int prevLogTerm;
            RaftLogEntry[] entries;

            lock (_lock)
            {
                if (!_nextIndex.TryGetValue(peerId, out var nextIndex))
                    return;

                prevLogIndex = nextIndex - 1;
                var prevEntry = GetLogEntry(prevLogIndex);
                prevLogTerm = prevEntry?.Term ?? 0;

                entries = _log.Where(e => e.Index >= nextIndex).ToArray();
            }

            var request = new AppendEntriesRequest(
                _currentTerm,
                NodeId,
                prevLogIndex,
                prevLogTerm,
                entries,
                _commitIndex
            );

            var response = await _transport.SendAppendEntriesAsync(peerId, request, ct);

            // Record successful contact with this peer
            _lastPeerContact[peerId] = DateTimeOffset.UtcNow;

            lock (_lock)
            {
                if (response.Term > _currentTerm)
                {
                    BecomeFollower(response.Term);
                    return;
                }

                if (response.Success)
                {
                    _nextIndex[peerId] = response.MatchIndex + 1;
                    _matchIndex[peerId] = response.MatchIndex;
                }
                else
                {
                    // Decrement nextIndex and retry
                    _nextIndex[peerId] = Math.Max(1, _nextIndex[peerId] - 1);
                }
            }
        }
        catch (Exception ex)
        {
            LogAppendEntriesError(peerId, ex);
        }
    }

    private void ApplyCommittedEntries()
    {
        while (_lastApplied < _commitIndex)
        {
            _lastApplied++;
            var entry = GetLogEntry(_lastApplied);
            if (entry != null && entry.CommandType != MetadataCommandType.Noop)
            {
                _stateMachine.Apply(entry);
                LogEntryApplied(entry.Index, entry.CommandType);
            }
        }
    }

    private RaftLogEntry? GetLogEntry(long index)
    {
        if (index <= 0 || index > _log.Count)
            return null;

        // Log is 1-indexed, list is 0-indexed
        return _log.FirstOrDefault(e => e.Index == index);
    }

    private void TruncateLogFrom(long index)
    {
        _log.RemoveAll(e => e.Index >= index);
    }

    private bool IsLogUpToDate(long lastLogIndex, int lastLogTerm)
    {
        var myLastTerm = LastLogTerm;
        var myLastIndex = LastLogIndex;

        if (lastLogTerm != myLastTerm)
            return lastLogTerm > myLastTerm;

        return lastLogIndex >= myLastIndex;
    }

    /// <summary>
    /// The voters other than this node — who to send to, and what to count acknowledgements
    /// from. Derived from the voter set rather than the transport, so membership has one
    /// source (#166).
    /// </summary>
    private IReadOnlyList<int> PeerIds
    {
        get
        {
            var voters = _voterSet.VoterIds;
            var peers = new List<int>(voters.Count);
            foreach (var id in voters)
            {
                if (id != _config.BrokerId) peers.Add(id);
            }
            return peers;
        }
    }

    /// <summary>
    /// Whether this node decides, or only follows along (#167).
    /// </summary>
    /// <remarks>
    /// False makes this node an observer: it receives and applies the metadata log, but never
    /// campaigns, never votes, and never counts towards a commit. With the transport-derived
    /// voter set this is always true — that set contains this node by construction — so an
    /// observer exists only where a quorum was stated explicitly and left this node out.
    /// </remarks>
    private bool IsVoter => _voterSet.VoterIds.Contains(_config.BrokerId);

    /// <summary>
    /// The nodes that receive the log without deciding it (#167).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Everything the transport can reach that the voter set does not name. The two questions
    /// are deliberately answered by different sources: the voter set states who decides, the
    /// transport reports who exists, and an observer is exactly the difference.
    /// </para>
    /// <para>
    /// Empty in combined mode, where every node is a voter — which is why this costs nothing
    /// for a single broker or an embedded host.
    /// </para>
    /// </remarks>
    private IReadOnlyList<int> ObserverIds
    {
        get
        {
            var voters = _voterSet.VoterIds;
            var known = _transport.GetPeerIds();

            List<int>? observers = null;
            foreach (var id in known)
            {
                if (id == _config.BrokerId || voters.Contains(id)) continue;
                (observers ??= []).Add(id);
            }

            return observers ?? (IReadOnlyList<int>)[];
        }
    }

    private void ResetElectionTimeout()
    {
        _electionTimeoutMs = _random.Next(
            _config.RaftElectionTimeoutMinMs,
            _config.RaftElectionTimeoutMaxMs
        );
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Raft node {NodeId} started (term={Term}, logSize={LogSize})")]
    private partial void LogRaftNodeStarted(int nodeId, int term, int logSize);

    [LoggerMessage(Level = LogLevel.Information, Message = "Voted for candidate {CandidateId} in term {Term}")]
    private partial void LogVotedFor(int candidateId, int term);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Appended {Count} entries, lastIndex={LastIndex}")]
    private partial void LogEntriesAppended(int count, long lastIndex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Proposed entry at index {Index}, type={CommandType}")]
    private partial void LogEntryProposed(long index, MetadataCommandType commandType);

    [LoggerMessage(Level = LogLevel.Information, Message = "Started election for term {Term}")]
    private partial void LogStartedElection(int term);

    [LoggerMessage(Level = LogLevel.Information, Message = "Waiting for peer discovery before becoming leader (term={Term}, expecting cluster peers)")]
    private partial void LogWaitingForPeerDiscovery(int term);

    [LoggerMessage(Level = LogLevel.Information, Message = "Waiting for peer discovery before enabling elections")]
    private partial void LogWaitingForPeerDiscoveryStart();

    [LoggerMessage(Level = LogLevel.Information, Message = "Peer {PeerId} is reachable, enabling elections")]
    private partial void LogPeerDiscovered(int peerId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Peer discovery timeout after {TimeoutSeconds}s, proceeding without peer confirmation")]
    private partial void LogPeerDiscoveryTimeout(int timeoutSeconds);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received vote from {PeerId}, total={Votes}/{Majority}")]
    private partial void LogReceivedVote(int peerId, int votes, int majority);

    // Pre-Vote logging
    [LoggerMessage(Level = LogLevel.Information, Message = "Starting pre-vote for proposed term {ProposedTerm}")]
    private partial void LogStartingPreVote(int proposedTerm);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received pre-vote from {PeerId}, total={PreVotes}/{Majority}")]
    private partial void LogReceivedPreVote(int peerId, int preVotes, int majority);

    [LoggerMessage(Level = LogLevel.Information, Message = "Pre-vote succeeded with {PreVotes}/{Majority} votes, proceeding to election")]
    private partial void LogPreVoteSucceeded(int preVotes, int majority);

    [LoggerMessage(Level = LogLevel.Information, Message = "Pre-vote failed with only {PreVotes}/{Majority} votes, aborting election")]
    private partial void LogPreVoteFailed(int preVotes, int majority);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Pre-vote rejected from {CandidateId}: active leader {LeaderId}")]
    private partial void LogPreVoteRejectedLeaderActive(int candidateId, int leaderId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Pre-vote granted to {CandidateId} for proposed term {ProposedTerm}")]
    private partial void LogPreVoteGranted(int candidateId, int proposedTerm);

    [LoggerMessage(Level = LogLevel.Information, Message = "Became follower in term {Term}")]
    private partial void LogBecameFollower(int term);

    [LoggerMessage(Level = LogLevel.Information, Message = "Became LEADER in term {Term}")]
    private partial void LogBecameLeader(int term);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Commit index advanced to {CommitIndex}")]
    private partial void LogCommitIndexAdvanced(long commitIndex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Applied entry {Index}, type={CommandType}")]
    private partial void LogEntryApplied(long index, MetadataCommandType commandType);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in election loop")]
    private partial void LogElectionLoopError(Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in heartbeat loop")]
    private partial void LogHeartbeatLoopError(Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Error sending AppendEntries to {PeerId}")]
    private partial void LogAppendEntriesError(int peerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Lost quorum connectivity - stepping down from leadership")]
    private partial void LogQuorumLost();

    [LoggerMessage(Level = LogLevel.Information, Message = "Stepping down from leadership in term {Term}")]
    private partial void LogSteppingDown(int term);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in quorum lost handler")]
    private partial void LogQuorumLostHandlerError(Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Graceful shutdown: not leader, proceeding immediately")]
    private partial void LogGracefulShutdownNotLeader();

    [LoggerMessage(Level = LogLevel.Information, Message = "Graceful shutdown initiated in term {Term}, stepping down from leadership")]
    private partial void LogGracefulShutdownInitiated(int term);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in graceful shutdown handler")]
    private partial void LogGracefulShutdownHandlerError(Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Graceful shutdown complete: new leader is broker {LeaderId}")]
    private partial void LogGracefulShutdownNewLeader(int leaderId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Graceful shutdown timeout: no new leader elected")]
    private partial void LogGracefulShutdownTimeout();
}

using System.Collections.Concurrent;
using System.Diagnostics;
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

    // The AppendEntries send each peer has under way. A peer gets its next one only when the last one landed, so a
    // slow or dead peer never holds up the rounds to the others (#204).
    private readonly ConcurrentDictionary<int, Task> _replicationInFlight = new();

    // Released by a proposal, so it goes out now rather than on the next heartbeat tick.
    private readonly SemaphoreSlim _replicateNow = new(0, 1);

    // Follows _commitIndex, advanced under _lock each time it moves: what WaitForCommitAsync and
    // WaitForCommitIndexAsync wait on. Costs no allocation per commit while nobody waits (#201).
    private readonly IndexWatermark _committed = new();

    // Whether the commit index has reached an entry of the term this node is in, noted under _lock; _caughtUp is
    // completed for it outside the lock, once the listeners know who leads.
    private bool _reachedCurrentTerm;
    private int _caughtUpSaid;
    private readonly TaskCompletionSource _caughtUp = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Timing
    private DateTimeOffset _lastHeartbeat = DateTimeOffset.UtcNow;

    // The election timer: when it was last restarted (a Stopwatch timestamp) and the timeout drawn for it then, both
    // under _lock. Restarted by a leader's AppendEntries, by a vote granted and by each campaign this node starts,
    // with a newly drawn timeout every time: the draw is what keeps two voters from campaigning in step.
    private long _electionTimerStarted;
    private int _electionTimeoutMs;

    // The pre-vote and the vote request a peer still has under way from an earlier round. A round ends with its
    // election timeout and leaves the requests nobody answered behind; the next one does not ask those peers again,
    // so a voter that is down has one request of each kind out, not one per round. Touched by the election loop only.
    private readonly Dictionary<int, Task> _preVoteInFlight = [];
    private readonly Dictionary<int, Task> _voteInFlight = [];

    // The persisted term, vote and log, read once: by LoadAsync, or by the first request that arrives before it.
    private readonly object _loadLock = new();
    private Task? _loaded;

    // The first voter this node heard from or reached since it started: what the election loop waits for where
    // peers are expected, see WaitForPeerAsync.
    private readonly TaskCompletionSource<int> _peerHeard = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Shutdown state
    private volatile bool _isShuttingDown;

    // What this node writes to its log and state files passes through here — a proposal, a vote, the entries of a
    // leader — and DisposeAsync closes it: a disposed node writes nothing, whoever still holds it.
    private readonly WriteGate _writes = new();

    /// <summary>
    /// Event raised when the leader detects it has lost quorum connectivity.
    /// </summary>
    public event EventHandler? OnQuorumLost;

    // Serialises the announcements, so that they reach the listeners in the order the changes happened. Taken before
    // _lock and never while holding it.
    private readonly object _announceLock = new();
    private RaftLeadership _announced;

    /// <summary>
    /// Raised when <see cref="Leadership"/> changed: this node won or lost the leadership, heard from a leader, or
    /// moved to another term (#180). Raised on the thread that made the change, after it released the node's lock and
    /// before it goes on — an AppendEntries from a new leader is answered only once the listeners know that leader —
    /// and in the order of the changes. A listener is told the moment the node knows, where it used to look every
    /// 100 ms.
    /// </summary>
    public event Action<RaftLeadership>? LeadershipChanged;

    /// <summary>Who leads, as this node knows it now: term, leader and whether it is this node, read as one.</summary>
    public RaftLeadership Leadership
    {
        get
        {
            lock (_lock)
            {
                return new RaftLeadership(_currentTerm, _leaderId, _state == RaftState.Leader);
            }
        }
    }

    // Tells the listeners what Leadership is now, if it is not what they were told last. Called after every change of
    // term, state or leader, outside _lock.
    private void AnnounceLeadership()
    {
        lock (_announceLock)
        {
            var now = Leadership;
            if (now == _announced)
                return;

            _announced = now;
            try
            {
                LeadershipChanged?.Invoke(now);
            }
            catch (Exception ex)
            {
                LogLeadershipChangedHandlerError(ex);
            }
        }
    }

    private CancellationTokenSource? _cts;
    private Task? _electionTask;
    private Task? _heartbeatTask;

    public int NodeId => _config.BrokerId;
    public int CurrentTerm => _currentTerm;
    public RaftState State => _state;
    public int? LeaderId => _leaderId;
    public bool IsLeader => _state == RaftState.Leader;
    public long CommitIndex => _commitIndex;
    /// <summary>
    /// Whether this node has caught up with the metadata log since it started; see
    /// <see cref="WaitForCaughtUpAsync"/>.
    /// </summary>
    public bool IsCaughtUp => _caughtUp.Task.IsCompletedSuccessfully;

    /// <summary>
    /// Whether this node is its whole quorum and expects no peer: it elects itself and applies its own log without
    /// any other node, so it catches up however the rest of a cluster fares.
    /// </summary>
    public bool CatchesUpAlone => IsVoter && PeerIds.Count == 0 && !ExpectsClusterPeers();

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

        // Who votes is a membership question, not a transport one (#166). An explicitly
        // configured quorum wins; without one the set is every broker the transport knows,
        // which is what this cluster did before the setting existed (#168).
        _voterSet = voterSet ?? BuildVoterSet(config, transport);

        RestartElectionTimer();
    }

    /// <summary>
    /// Picks the voter set from configuration, so both the hosted broker and the embedded
    /// runtime get the same answer without either having to assemble it (#168).
    /// </summary>
    private IRaftVoterSet BuildVoterSet(ClusteringConfig config, IRaftTransport transport)
    {
        // Startup does not run the config validator — that is reachable through
        // /api/config/validate — so an unreadable entry has to be said here or it is said
        // nowhere. Logged rather than thrown: refusing to start over one bad entry would take
        // a broker down for a setting that is new, and the remaining voters still form a
        // quorum if they are a majority.
        var parseErrors = new List<string>();
        var voters = ControllerQuorum.Parse(config.ControllerQuorumVoters, parseErrors);

        foreach (var parseError in parseErrors)
            LogQuorumEntryIgnored(parseError);

        if (voters.Count == 0)
        {
            // Either nothing was configured, or nothing in it could be read.
            if (parseErrors.Count > 0)
                LogQuorumUnusable();

            // An undeclared quorum means "I am the quorum", not "whoever shows up votes". The set
            // used to be derived from the transport and therefore GREW as brokers registered: a
            // seed that started alone found itself needing a majority of nodes that never vote,
            // and lost the leadership it already held (#176). A cluster of more than one broker
            // has to declare its voters (#172), so this branch is the lone broker's.
            return new ConfiguredVoterSet([config.BrokerId]);
        }

        if (ControllerQuorum.IsEvenVoterCount(voters))
            LogEvenVoterCount(voters.Count, voters.Count - 1);

        var voterIds = voters.Select(v => v.NodeId).ToArray();
        LogConfiguredQuorum(voterIds.Length, string.Join(", ", voterIds), voterIds.Contains(config.BrokerId));

        return new ConfiguredVoterSet(voterIds);
    }

    /// <summary>
    /// Reads the term, the vote and the log this node persisted, once; a second call waits for the first one's
    /// reading. A log that cannot be read is thrown here, and by every call after it: the node does not start on it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The first step of a broker's start, before the port its peers reach this node on is opened (#275): load, open
    /// the port, <see cref="StartAsync"/>. A node is constructed with term 0, no vote and an empty log, and a request
    /// answered from that grants a second vote in a term the node voted in before it stopped, and takes the entries
    /// of a leader deposed long ago.
    /// </para>
    /// <para>
    /// A request that arrives before this was called is not answered from the constructed state either: the handlers
    /// read the persisted state first, by the same single reading.
    /// </para>
    /// </remarks>
    public Task LoadAsync(CancellationToken cancellationToken = default)
    {
        var loaded = EnsureLoaded();
        return loaded.IsCompletedSuccessfully ? loaded : loaded.WaitAsync(cancellationToken);
    }

    private Task EnsureLoaded()
    {
        lock (_loadLock)
        {
            // Off the caller's thread and under no caller's token: the one reading is every caller's.
            return _loaded ??= Task.Run(LoadPersistedStateAsync);
        }
    }

    private async Task LoadPersistedStateAsync()
    {
        var state = await _persistence.LoadStateAsync(CancellationToken.None).ConfigureAwait(false);
        var entries = await _persistence.LoadLogAsync(CancellationToken.None).ConfigureAwait(false);

        lock (_lock)
        {
            _currentTerm = state.CurrentTerm;
            _votedFor = state.VotedFor;
            _log.AddRange(entries);
        }
    }

    /// <summary>
    /// Starts the node's elections, on the persisted state (see <see cref="LoadAsync"/>, which this calls if the host
    /// has not). Returns once they are under way: it does not wait for a peer.
    /// </summary>
    /// <remarks>
    /// Where peers are expected the first election waits until one was heard from or reached, at most
    /// <see cref="ClusteringConfig.RaftPeerDiscoveryTimeoutSeconds"/>. That wait used to be part of this call, and the
    /// broker host opened its replication port only after it: brokers started together each waited for a port none
    /// of them had opened yet, the whole 30 s (#270), and a broker started alone bound its HTTP port, health
    /// endpoints included, only after them.
    /// </remarks>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await LoadAsync(cancellationToken);

        _electionTask = Task.Run(() => RunElectionsAsync(_cts.Token), _cts.Token);

        LogRaftNodeStarted(NodeId, _currentTerm, _log.Count);
    }

    private async Task RunElectionsAsync(CancellationToken ct)
    {
        try
        {
            await WaitForPeerAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return;
        }

        await ElectionTimeoutLoopAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        // Whoever still waits for a node that never caught up is told so.
        _caughtUp.TrySetCanceled();

        // The stop's handshake with the node's writers: none is admitted from here on, and those under way have
        // written what they write when this returns. The hosts dispose the node before the controller and the
        // replication server, which went on proposing and passing on a leader's entries, and the node went on
        // appending them to raft-log.bin — while the broker started next on that directory was loading it.
        await _writes.CloseAsync().ConfigureAwait(false);

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

        foreach (var send in _replicationInFlight.Values)
        {
            try { await send.ConfigureAwait(false); } catch (OperationCanceledException) { }
        }

        _cts.Dispose();
        _cts = null;
        _replicateNow.Dispose();
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
        // Answered from the term and the log this node persisted, never from the ones it was constructed with.
        var loaded = EnsureLoaded();
        return loaded.IsCompletedSuccessfully ? Task.FromResult(DecidePreVote(request)) : AfterLoadAsync();

        async Task<PreVoteResponse> AfterLoadAsync()
        {
            await loaded.WaitAsync(ct).ConfigureAwait(false);
            return DecidePreVote(request);
        }
    }

    private PreVoteResponse DecidePreVote(PreVoteRequest request)
    {
        NotePeerHeard(request.CandidateId);

        lock (_lock)
        {
            // An observer is not part of the quorum, so its answer must not be counted
            // towards one (#167). A candidate does not ask it; refusing is the guard for a
            // stale voter list on the other side, where granting would let a minority of
            // real voters plus observers elect a leader.
            if (!IsVoter)
            {
                return new PreVoteResponse(_currentTerm, false);
            }

            // If proposed term is less than our current term, reject
            if (request.ProposedTerm < _currentTerm)
            {
                return new PreVoteResponse(_currentTerm, false);
            }

            // A leader that still has quorum does not help elect a rival. Pre-vote exists to stop a
            // partitioned node from disrupting a healthy term, and the leader is the node that knows
            // the term is healthy — but it never refreshes its own _lastHeartbeat, because it
            // receives no heartbeats, so the "heard from a leader recently" check below always
            // passes for it and it grants the pre-vote.
            //
            // That was unreachable while the push model carried metadata and Raft was off by
            // default. With the log as the only model (#163 step 3) it is on the main path, and it
            // is how a cluster ends up with a leader per broker: each newcomer campaigns, the
            // sitting leader votes for it, and every node believes it is the controller.
            //
            // Gated on quorum connectivity rather than on being leader alone: a leader that has
            // LOST its quorum must not be able to block the election that replaces it.
            if (_state == RaftState.Leader && HasQuorumConnectivity)
            {
                LogPreVoteRejectedLeaderActive(request.CandidateId, NodeId);
                return new PreVoteResponse(_currentTerm, false);
            }

            // Check if we've heard from a leader recently
            // If so, reject to prevent disrupting a working cluster
            var timeSinceLastHeartbeat = (DateTimeOffset.UtcNow - _lastHeartbeat).TotalMilliseconds;
            if (_leaderId.HasValue && timeSinceLastHeartbeat < _config.RaftElectionTimeoutMinMs)
            {
                LogPreVoteRejectedLeaderActive(request.CandidateId, _leaderId.Value);
                return new PreVoteResponse(_currentTerm, false);
            }

            // Grant pre-vote if candidate's log is at least as up-to-date as ours
            var canVote = IsLogUpToDate(request.LastLogIndex, request.LastLogTerm);

            if (canVote)
            {
                LogPreVoteGranted(request.CandidateId, request.ProposedTerm);
            }

            // Note: Pre-Vote does NOT persist state changes (no term update, no votedFor)
            return new PreVoteResponse(_currentTerm, canVote);
        }
    }

    /// <summary>
    /// Handle RequestVote RPC from a candidate.
    /// </summary>
    public async Task<RequestVoteResponse> HandleRequestVoteAsync(RequestVoteRequest request, CancellationToken ct)
    {
        // Answered from the term and the vote this node persisted, never from the ones it was constructed with.
        await LoadAsync(ct).ConfigureAwait(false);
        NotePeerHeard(request.CandidateId);

        // A disposed node grants no vote: it could not persist it.
        if (!_writes.TryEnter())
            return new RequestVoteResponse(_currentTerm, false);

        try
        {
            return await DecideVoteAsync(request, ct).ConfigureAwait(false);
        }
        finally
        {
            _writes.Exit();
        }
    }

    private async Task<RequestVoteResponse> DecideVoteAsync(RequestVoteRequest request, CancellationToken ct)
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
                RestartElectionTimer();
                stateChanged = true;
                LogVotedFor(NodeId, request.CandidateId, _currentTerm);
            }

            // Capture state for persistence
            termToSave = _currentTerm;
            votedForToSave = _votedFor;
            response = new RequestVoteResponse(_currentTerm, canVote);
        }

        AnnounceLeadership();

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
        // Matched against the log this node persisted, never against the empty one it was constructed with.
        await LoadAsync(ct).ConfigureAwait(false);
        NotePeerHeard(request.LeaderId);

        // A disposed node takes no entry: it could not persist it.
        if (!_writes.TryEnter())
            return new AppendEntriesResponse(_currentTerm, false, 0);

        try
        {
            return await FollowLeaderAsync(request, ct).ConfigureAwait(false);
        }
        finally
        {
            _writes.Exit();
        }
    }

    private async Task<AppendEntriesResponse> FollowLeaderAsync(AppendEntriesRequest request, CancellationToken ct)
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
            RestartElectionTimer();

            stateChanged = false;

            // Another leader of this very term: election safety is broken, which Raft rules out. Said loudly, then
            // this node follows — staying leader while naming the other one as leader was the state of #213.
            if (_state == RaftState.Leader && request.Term == _currentTerm && request.LeaderId != NodeId)
            {
                LogElectionSafetyViolation(_currentTerm, request.LeaderId);
                BecomeFollower(request.Term);
                stateChanged = true;
            }

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
                _committed.Advance(_commitIndex);
                NoteCommitIndexMoved();
            }

            response = new AppendEntriesResponse(_currentTerm, true, LastLogIndex);
        }

    PersistAndReturn:
        AnnounceLeadership();

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

        SignalCaughtUp();
        return response;
    }

    /// <summary>
    /// Propose a new command to be replicated (leader only).
    /// Returns the log index if successful, or -1 if not leader, or if the node was disposed.
    /// </summary>
    public async Task<long> ProposeAsync(MetadataCommandType commandType, byte[] data, CancellationToken ct)
    {
        RaftLogEntry? entryToSave;
        long resultIndex;

        lock (_lock)
        {
            // Admitted with the entry, under the lock: a disposed node holds no entry it did not write.
            if (_state != RaftState.Leader || !_writes.TryEnter())
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

        try
        {
            // Persist log entry before considering it proposed (Raft correctness requirement)
            await _persistence.AppendEntriesAsync([entryToSave], ct);
        }
        finally
        {
            _writes.Exit();
        }

        // Commit what is already on a majority, here rather than on the next heartbeat tick
        // (#177). For a single-voter quorum this entry IS on a majority the moment it is
        // persisted — the leader's own log is the majority — and the difference is not academic:
        // the commit index a follower learns is the one carried in AppendEntries, so a commit
        // that lands a tick late means the FIRST request carrying the entry tells the follower
        // to store it and not to apply it. If the leader goes away before the following tick,
        // the follower holds a committed decision it never acts on. Advancing here closes that
        // window; for a multi-voter quorum this finds nothing new and costs a lock.
        TryAdvanceCommitIndex(PeerIds);

        // Replicated by the heartbeat loop, woken for it now rather than on its next tick: a caller that waits for
        // the commit would otherwise wait a heartbeat interval per entry (#204).
        WakeReplication();
        return resultIndex;
    }

    /// <summary>
    /// Wait for a log entry to be committed.
    /// </summary>
    /// <remarks>
    /// Woken by the commit index moving, not by polling it; the timeout is only the bound for an entry that never
    /// commits, such as one a leader that lost its office proposed.
    /// </remarks>
    public async Task<bool> WaitForCommitAsync(long index, TimeSpan timeout, CancellationToken ct)
    {
        if (_committed.Value >= index)
            return true;

        if (ct.IsCancellationRequested || timeout <= TimeSpan.Zero)
            return false;

        // The timeout cancels the wait rather than abandoning it, so a wait that times out leaves no waiter behind.
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(timeout);
        try
        {
            await _committed.WaitAsync(index, bounded.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return _committed.Value >= index;
        }
    }

    /// <summary>
    /// Completes when this node's commit index reaches <paramref name="index"/>: at once if it already has.
    /// </summary>
    /// <remarks>
    /// On a follower or observer the commit index is what the leader's AppendEntries said, capped at the node's own
    /// log end, so reaching <paramref name="index"/> means the node holds the leader's entries up to it (Raft's log
    /// matching) and has applied them (#201). Not bounded: a caller that may wait for an entry that never commits
    /// bounds the wait with <paramref name="ct"/>, or uses <see cref="WaitForCommitAsync"/>.
    /// </remarks>
    public Task WaitForCommitIndexAsync(long index, CancellationToken ct) => _committed.WaitAsync(index, ct);

    /// <summary>
    /// Completes when this node has caught up with the metadata log since it started: at once if it already has.
    /// Cancelled with <paramref name="ct"/>, and when the node is disposed without having got there.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A node starts with its log loaded and nothing of it applied — the commit index is not persisted — so until
    /// then its state machine is empty, or, while it applies, somewhere in the middle of the log. A broker that
    /// answered a client in that time said a topic created before the restart did not exist, or described it without
    /// a config change made after its creation.
    /// </para>
    /// <para>
    /// Caught up means: the commit index has reached an entry of the term the node is in, and everything up to it is
    /// applied. A leader gets there when the no-op it appends on taking office commits, which commits and applies
    /// the whole log it started with. A follower or observer gets there with the first AppendEntries of the leader
    /// that carries a commit index at or past that leader's no-op: it matched the follower's log against the
    /// leader's and brought every entry the leader had, so the follower has applied what the leader had committed
    /// when it sent it. A commit index a new leader passes on before its own no-op commits says nothing yet — after
    /// a restart of the whole cluster it is 0 — and does not count.
    /// </para>
    /// <para>
    /// Not bounded: without a quorum there is no leader, no commit and no catching up, and the node says so by not
    /// completing this. Once, for the life of the node: a node that falls behind later is not reported here.
    /// </para>
    /// </remarks>
    public Task WaitForCaughtUpAsync(CancellationToken ct) => _caughtUp.Task.WaitAsync(ct);

    // Under _lock, each time the commit index moved and the entries up to it are applied.
    private void NoteCommitIndexMoved()
    {
        if (!_reachedCurrentTerm && GetLogEntry(_commitIndex)?.Term == _currentTerm)
            _reachedCurrentTerm = true;
    }

    // Outside _lock, after a step that may have moved the commit index. The listeners are told who leads first: a
    // broker that reports itself caught up knows its controller, and the one that leads knows it is the controller.
    private void SignalCaughtUp()
    {
        if (_caughtUp.Task.IsCompleted)
            return;

        long index;
        int term;
        lock (_lock)
        {
            if (!_reachedCurrentTerm)
                return;
            index = _commitIndex;
            term = _currentTerm;
        }

        AnnounceLeadership();

        // Said before it is signalled, so that it precedes in the log whatever a waiter says next.
        if (Interlocked.Exchange(ref _caughtUpSaid, 1) != 0)
            return;
        LogCaughtUp(NodeId, index, term);
        _caughtUp.TrySetResult();
    }

    /// <summary>
    /// The entry this node's log holds at <paramref name="index"/>, if any — test-only (via InternalsVisibleTo), so
    /// a test can check that a follower holds the very entry the leader proposed.
    /// </summary>
    internal RaftLogEntry? LogEntryAt(long index)
    {
        lock (_lock)
        {
            return GetLogEntry(index);
        }
    }

    /// <summary>
    /// Wait until every node in <paramref name="nodeIds"/> holds the log up to
    /// <paramref name="index"/>. Returns <see langword="false"/> if the deadline passes first.
    /// </summary>
    /// <remarks>
    /// Committing is not delivering (#177). On a single-voter quorum an entry commits the moment
    /// it is appended — the leader's own log is the majority — while the peers and observers that
    /// have to ACT on it only receive it on the next heartbeat tick. That gap is harmless while
    /// the leader keeps running, and fatal when it is leaving: a departing controller that hands
    /// a partition to its successor commits the decision, exits, and takes the only copy with it.
    /// So a caller that is about to walk away asks this instead of asking whether it committed.
    /// <para>
    /// Named nodes only, deliberately: waiting on everyone would let one unreachable peer spend
    /// the whole shutdown budget (the mirror of #176's commit stall), and the nodes that matter
    /// here are the ones that must act — the successors.
    /// </para>
    /// </remarks>
    public async Task<bool> WaitForReplicationAsync(
        long index,
        IReadOnlyList<int> nodeIds,
        TimeSpan timeout,
        CancellationToken ct = default)
    {
        if (nodeIds.Count == 0)
            return true;

        var deadline = DateTimeOffset.UtcNow + timeout;

        while (!ct.IsCancellationRequested)
        {
            if (AllHold(index, nodeIds))
                return true;

            if (DateTimeOffset.UtcNow >= deadline)
                break;

            await Task.Delay(10, ct);
        }

        return AllHold(index, nodeIds);
    }

    private bool AllHold(long index, IReadOnlyList<int> nodeIds)
    {
        foreach (var nodeId in nodeIds)
        {
            if (GetPeerMatchIndex(nodeId) < index)
                return false;
        }

        return true;
    }

    private async Task ElectionTimeoutLoopAsync(CancellationToken ct)
    {
        // The first election comes an election timeout after elections were enabled, as every later one comes an
        // election timeout after the last sign of a leader. The timer used to run from the node's construction, so
        // voters that had waited out the peer discovery together all campaigned on their first tick, in step. A node
        // that is its whole quorum has nobody to be in step with and elects itself as early as it did.
        if (PeerIds.Count > 0)
        {
            lock (_lock)
            {
                RestartElectionTimer();
            }
        }

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

                if (ElectionTimeoutElapsed())
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

    // Under _lock. A pre-vote speaks for the term it proposed and the silence it started in: once the term moved on,
    // this node leads, or a leader was heard from since, its result says nothing about now.
    private bool IsStalePreVote(int proposedTerm, DateTimeOffset started) =>
        _currentTerm + 1 != proposedTerm
        || _state == RaftState.Leader
        || (_leaderId.HasValue && _leaderId != NodeId && _lastHeartbeat > started);

    private async Task StartElectionAsync(CancellationToken ct)
    {
        try
        {
            await CampaignAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            // However the campaign ended: as leader, as follower of a newer term, or as a candidate nobody answered.
            AnnounceLeadership();

            // A lone voter committed its no-op while it took office.
            SignalCaughtUp();
        }
    }

    private async Task CampaignAsync(CancellationToken ct)
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

        // A campaign's round lasts one election timeout at most, and the next campaign starts a newly drawn timeout
        // after this one started, whatever became of it: lost, refused or not answered.
        TimeSpan preVoteTimeout;
        lock (_lock)
        {
            preVoteTimeout = RestartElectionTimer();
        }

        // Split-brain prevention: Don't start election when peers are expected but not yet discovered.
        if (peers.Count == 0 && ExpectsClusterPeers())
        {
            LogWaitingForPeerDiscovery(_currentTerm);
            return;
        }

        // No round that is lost before it is asked. A peer still busy with the request of an earlier round is not asked
        // again, and where the peers left to ask are short of a majority with this node — a voter alone, every peer
        // down — the next round waits for the connect attempts under way to give up.
        if (!CanDecide(_preVoteInFlight, peers, majority) || !CanDecide(_voteInFlight, peers, majority))
        {
            LogCampaignAwaitsAnswers(proposedTerm);
            return;
        }

        LogStartingPreVote(proposedTerm);

        var preVoteRequest = new PreVoteRequest(
            proposedTerm,
            NodeId,
            LastLogIndex,
            LastLogTerm
        );

        var preVoteStarted = DateTimeOffset.UtcNow;
        var preVotes = 1; // Pre-vote for self
        var preVoteResults = await CollectUntilDecidedAsync(
            peers, _preVoteInFlight, peerId => _transport.SendPreVoteAsync(peerId, preVoteRequest, ct),
            r => r.VoteGranted, r => r.Term > proposedTerm - 1, majority, preVoteTimeout, ct).ConfigureAwait(false);

        lock (_lock)
        {
            // The answers can take seconds — a peer that is not up yet costs its connect retries — and the world
            // may have moved on meanwhile: a leader elected, this node its follower. Acting on the stale result
            // started a second election right after the first and deposed a leader that was working (#204).
            if (IsStalePreVote(proposedTerm, preVoteStarted))
            {
                LogPreVoteStale(proposedTerm, _currentTerm);
                return;
            }

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

            LogPreVoteSucceeded(NodeId, preVotes, majority);
        }

        // ========================================
        // Phase 2: Real Election (standard Raft)
        // ========================================
        // We got enough pre-votes, now start the actual election

        int termToSave;
        int votedForToSave;
        TimeSpan electionTimeout;

        lock (_lock)
        {
            // The lock was let go after counting; a leader may have spoken up in between.
            if (IsStalePreVote(proposedTerm, preVoteStarted))
            {
                LogPreVoteStale(proposedTerm, _currentTerm);
                return;
            }

            _state = RaftState.Candidate;
            _currentTerm++;
            _votedFor = NodeId;
            _lastHeartbeat = DateTimeOffset.UtcNow;
            electionTimeout = RestartElectionTimer();

            termToSave = _currentTerm;
            votedForToSave = NodeId;
        }

        // Persist state before sending RequestVote RPCs (Raft correctness requirement)
        await _persistence.SaveStateAsync(termToSave, votedForToSave, ct);

        LogStartedElection(NodeId, _currentTerm);

        var votes = 1; // Vote for self

        var request = new RequestVoteRequest(
            _currentTerm,
            NodeId,
            LastLogIndex,
            LastLogTerm
        );

        // Without a majority by its election timeout the election is over (Raft §5.2): this node stays a candidate and
        // the loop starts the next election, for the next term and with a timeout of its own.
        var results = await CollectUntilDecidedAsync(
            peers, _voteInFlight, peerId => _transport.SendRequestVoteAsync(peerId, request, ct),
            r => r.VoteGranted, r => r.Term > request.Term, majority, electionTimeout, ct).ConfigureAwait(false);

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
    /// Asks the peers and returns the answers that decided the round (#225): once this node and the grants are a
    /// majority, once the peers still to answer could no longer make one, or once an answer names a newer term — and
    /// the answers there are when <paramref name="timeout"/>, the election timeout drawn for this round, is over.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A peer that cannot be reached answers only when its connect attempts gave up, seconds later, and that is the
    /// same number of seconds for every voter that asks it. A round that waited for it — with two of three voters up
    /// and each voting for itself, the third could still have decided the round — ended for both voters at that
    /// moment, long past either one's election timeout, so both started the next election on their next tick: in step
    /// again, each for itself, term after term, and the timeouts they had drawn never came into it.
    /// </para>
    /// <para>
    /// The requests still out when the round ends go on and their answers are not counted. A peer whose request from
    /// an earlier round is still under way is not asked again: it has not answered that one.
    /// </para>
    /// </remarks>
    private static async Task<List<(int PeerId, TResponse? Response)>> CollectUntilDecidedAsync<TResponse>(
        IReadOnlyList<int> peers, Dictionary<int, Task> inFlight, Func<int, Task<TResponse>> send,
        Func<TResponse, bool> granted, Func<TResponse, bool> newerTerm, int majority, TimeSpan timeout,
        CancellationToken ct)
        where TResponse : class
    {
        var pending = new List<Task<(int PeerId, TResponse? Response)>>(peers.Count);
        foreach (var peerId in peers)
        {
            if (inFlight.TryGetValue(peerId, out var underWay) && !underWay.IsCompleted)
                continue;

            var ask = AskAsync(peerId, send);
            inFlight[peerId] = ask;
            pending.Add(ask);
        }

        var answers = new List<(int PeerId, TResponse? Response)>(pending.Count);
        var grants = 1; // this node's own

        // The round's election timer, stopped when the round is decided before it runs out.
        using var timer = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var timedOut = Task.Delay(timeout, timer.Token);
        try
        {
            while (pending.Count > 0 && grants < majority && grants + pending.Count >= majority)
            {
                var answered = Task.WhenAny(pending);
                if (await Task.WhenAny(answered, timedOut).ConfigureAwait(false) == timedOut)
                    break;

                var done = await answered.ConfigureAwait(false);
                pending.Remove(done);
                var answer = await done.ConfigureAwait(false);
                answers.Add(answer);
                if (answer.Response is not { } response)
                    continue;
                if (newerTerm(response))
                    break;
                if (granted(response))
                    grants++;
            }
        }
        finally
        {
            await timer.CancelAsync().ConfigureAwait(false);
        }
        return answers;
    }

    // Whether this node and the peers a round would ask — those with no request of that kind under way — are a majority.
    private static bool CanDecide(Dictionary<int, Task> inFlight, IReadOnlyList<int> peers, int majority)
    {
        var votes = 1; // this node's own
        foreach (var peerId in peers)
        {
            if (!inFlight.TryGetValue(peerId, out var underWay) || underWay.IsCompleted)
                votes++;
        }
        return votes >= majority;
    }

    // One peer's answer, or none if the request failed.
    private static async Task<(int PeerId, TResponse? Response)> AskAsync<TResponse>(
        int peerId, Func<int, Task<TResponse>> send)
        where TResponse : class
    {
        try
        {
            return (peerId, await send(peerId).ConfigureAwait(false));
        }
        catch
        {
            return (peerId, null);
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
    /// Tells the node that a broker was heard from or reached. The first voter other than this node ends the wait
    /// for a peer that precedes the first election; see <see cref="WaitForPeerAsync"/>.
    /// </summary>
    /// <remarks>
    /// Called by the node for every Raft request it is sent and for the peer it reached itself, and by the
    /// replication server for the broker heartbeats that arrive on the same port.
    /// </remarks>
    public void NotePeerHeard(int nodeId)
    {
        if (_peerHeard.Task.IsCompleted || nodeId == NodeId || !_voterSet.VoterIds.Contains(nodeId))
            return;

        _peerHeard.TrySetResult(nodeId);
    }

    /// <summary>
    /// Waits, where peers are expected, until one voter was heard from or reached before the first election, at most
    /// <see cref="ClusteringConfig.RaftPeerDiscoveryTimeoutSeconds"/>: a broker started before the others does not
    /// campaign, and log, round after round while it is alone.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A handshake, where it was a connect attempt to every peer every 100 ms (#267): each peer is tried once, and
    /// after that the node is told — by the first Raft request or broker heartbeat a voter sends it. Every broker
    /// opens its replication port before it gets here, so of two brokers the one that tries later finds the other's
    /// port open, starts its elections and asks the other for a pre-vote, and the brokers' heartbeats, which go on
    /// for as long as they run, tell a node whose peers came up after its attempt.
    /// </para>
    /// <para>
    /// The requests of a peer are answered throughout: this delays only the elections this node starts.
    /// </para>
    /// </remarks>
    private async Task WaitForPeerAsync(CancellationToken ct)
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

        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(TimeSpan.FromSeconds(_config.RaftPeerDiscoveryTimeoutSeconds));

        // Not waited for: an attempt ends by itself, reached or not, and with this wait at the latest.
        foreach (var peerId in peers)
            _ = TryReachAsync(peerId, bounded.Token);

        try
        {
            var peer = await _peerHeard.Task.WaitAsync(bounded.Token).ConfigureAwait(false);
            LogPeerDiscovered(peer);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            LogPeerDiscoveryTimeout(_config.RaftPeerDiscoveryTimeoutSeconds);
        }
        finally
        {
            await bounded.CancelAsync().ConfigureAwait(false);
        }
    }

    // One attempt to reach a peer; it is heard if the attempt succeeds, and nothing is said if it does not.
    private async Task TryReachAsync(int peerId, CancellationToken ct)
    {
        try
        {
            if (await _transport.IsPeerReachableAsync(peerId, ct).ConfigureAwait(false))
                NotePeerHeard(peerId);
        }
        catch (Exception ex)
        {
            LogPeerNotReached(peerId, ex);
        }
    }

    private void BecomeFollower(int term)
    {
        _state = RaftState.Follower;

        // The vote is per term: only a newer term frees it (#213). A candidate that hears from the leader of its own
        // term, or a leader that steps down, used to forget the vote it had cast in that term and could cast another
        // — for a node starting its campaign for the same term from the one before — and two nodes led one term.
        if (term > _currentTerm)
        {
            _currentTerm = term;
            _votedFor = null;
        }
        _leaderId = null;

        // Cancel heartbeat task if running
        LogBecameFollower(NodeId, term);
    }

    private void BecomeLeader()
    {
        _state = RaftState.Leader;
        _leaderId = NodeId;

        // Initialize leader state
        _nextIndex.Clear();
        _matchIndex.Clear();

        var tookOffice = DateTimeOffset.UtcNow;
        foreach (var peerId in PeerIds)
        {
            _nextIndex[peerId] = LastLogIndex + 1;
            _matchIndex[peerId] = 0;
            // The isolation check counts from here: the heartbeat loop no longer waits for the first round's answers
            // before it looks, so a peer gets one isolation timeout from the election to answer (#204).
            _lastPeerContact[peerId] = tookOffice;
        }

        // Observers are replicated to on the same terms as voters — they just do not count
        // (#167). Without this they would have no nextIndex and SendAppendEntriesAsync would
        // skip them, which is a broker with no metadata log at all.
        foreach (var observerId in ObserverIds)
        {
            _nextIndex[observerId] = LastLogIndex + 1;
            _matchIndex[observerId] = 0;
        }

        LogBecameLeader(NodeId, _currentTerm);

        // Start heartbeat loop
        _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(_cts!.Token));

        // Append no-op entry to establish leadership
        _ = ProposeAsync(MetadataCommandType.Noop, [], CancellationToken.None);
    }

    private async Task HeartbeatLoopAsync(CancellationToken ct)
    {
        // The listeners learn that this node leads before any peer does: its broker is the controller by the time a
        // follower hears from it and names it (#180).
        AnnounceLeadership();

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

                // The heartbeat interval, or less when a proposal has something to send.
                await _replicateNow.WaitAsync(_config.RaftHeartbeatIntervalMs, ct).ConfigureAwait(false);
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

            // A follower from here, with a follower's election timeout ahead of it: the timer was last restarted by
            // the campaign this node won, and left as it was the node campaigned on its next tick.
            RestartElectionTimer();
        }

        AnnounceLeadership();

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

    private Task SendHeartbeatsAsync(CancellationToken ct)
    {
        var peers = PeerIds;
        var observers = ObserverIds;

        // A node that joined mid-term has no replication state yet, because BecomeLeader ran
        // before it existed. Seeding it here rather than skipping it is what lets a broker
        // start into a running cluster and receive the log (#167).
        EnsureReplicationState(peers);
        EnsureReplicationState(observers);

        // Once before any send: a lone voter is its own majority and has no peer whose
        // completion could ever trigger the check after one.
        TryAdvanceCommitIndex(peers);
        SignalCaughtUp();

        // Each peer on its own: the round does not wait for the sends, and a peer whose last send is still under way
        // is skipped. A majority is what commits an entry, and waiting for the rest hands a single unreachable peer
        // the power to hold up the commit: its send burns three attempts with a 3 s connect and a 5 s RPC bound
        // (#176). Committing as each send landed was not enough while the next round still waited for all of them —
        // with a broker dead, the leader sent a round every ten seconds or so, and a controller handling the failure
        // one partition at a time took minutes (#204).
        foreach (var peerId in peers.Concat(observers))
        {
            if (_replicationInFlight.TryGetValue(peerId, out var underWay) && !underWay.IsCompleted)
                continue;
            _replicationInFlight[peerId] = ReplicateToAsync(peerId, ct);
        }

        return Task.CompletedTask;
    }

    private async Task ReplicateToAsync(int peerId, CancellationToken ct)
    {
        try
        {
            await SendAppendEntriesAsync(peerId, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            LogHeartbeatLoopError(ex);
            return;
        }

        TryAdvanceCommitIndex(PeerIds);
        SignalCaughtUp();
    }

    // Lets the heartbeat loop run its next round now; at most one wake-up is pending.
    private void WakeReplication()
    {
        try
        {
            if (_replicateNow.CurrentCount == 0)
                _replicateNow.Release();
        }
        catch (SemaphoreFullException)
        {
            // Another proposal woke it first.
        }
        catch (ObjectDisposedException)
        {
            // Disposed; nothing left to replicate.
        }
    }

    /// <summary>
    /// Moves the commit index up to the highest entry a majority of the VOTERS holds.
    /// </summary>
    /// <remarks>
    /// Voters only (#167). Counting observers would let their acknowledgements stand in for a
    /// quorum: three observers ahead of two voters would commit an entry that only one voter
    /// holds, and losing that voter loses committed metadata.
    /// </remarks>
    private void TryAdvanceCommitIndex(IReadOnlyList<int> voters)
    {
        lock (_lock)
        {
            if (_state != RaftState.Leader)
                return;

            var matchIndices = new List<long>(voters.Count + 1) { LastLogIndex };
            foreach (var voterId in voters)
            {
                matchIndices.Add(_matchIndex.TryGetValue(voterId, out var match) ? match : 0);
            }

            matchIndices.Sort();
            matchIndices.Reverse();
            var majorityIndex = matchIndices.Count / 2;
            var newCommitIndex = matchIndices[majorityIndex];

            if (newCommitIndex <= _commitIndex)
                return;

            // Raft only commits an entry from the CURRENT term on the strength of a majority;
            // an older one follows once a current-term entry does.
            var entry = GetLogEntry(newCommitIndex);
            if (entry is null || entry.Term != _currentTerm)
                return;

            _commitIndex = newCommitIndex;
            LogCommitIndexAdvanced(_commitIndex);
            ApplyCommittedEntries();
            _committed.Advance(_commitIndex);
            NoteCommitIndexMoved();
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

            var deposed = false;
            lock (_lock)
            {
                if (response.Term > _currentTerm)
                {
                    BecomeFollower(response.Term);
                    deposed = true;
                }
                else if (response.Success)
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

            if (deposed)
                AnnounceLeadership();
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

    // Under _lock, or before the node is started. Restarts the election timer with a newly drawn timeout and returns
    // that timeout.
    private TimeSpan RestartElectionTimer()
    {
        _electionTimerStarted = Stopwatch.GetTimestamp();
        _electionTimeoutMs = _random.Next(
            _config.RaftElectionTimeoutMinMs,
            _config.RaftElectionTimeoutMaxMs
        );
        return TimeSpan.FromMilliseconds(_electionTimeoutMs);
    }

    private bool ElectionTimeoutElapsed()
    {
        lock (_lock)
        {
            return Stopwatch.GetElapsedTime(_electionTimerStarted).TotalMilliseconds >= _electionTimeoutMs;
        }
    }

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Ignoring an entry in the controller quorum: {Error}")]
    private partial void LogQuorumEntryIgnored(string error);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "No usable voter in the configured controller quorum; falling back to combined mode, "
            + "where every broker votes")]
    private partial void LogQuorumUnusable();

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Controller quorum configured with {VoterCount} voters ({VoterIds}); this node votes: {IsVoter}")]
    private partial void LogConfiguredQuorum(int voterCount, string voterIds, bool isVoter);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Controller quorum has an even number of voters ({VoterCount}); it tolerates the same "
            + "number of failures as {OddCount} would, so the extra voter adds a node that can fail without "
            + "adding a failure the quorum survives")]
    private partial void LogEvenVoterCount(int voterCount, int oddCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Raft node {NodeId} started (term={Term}, logSize={LogSize})")]
    private partial void LogRaftNodeStarted(int nodeId, int term, int logSize);

    [LoggerMessage(Level = LogLevel.Information, Message = "[node {NodeId}] Voted for candidate {CandidateId} in term {Term}")]
    private partial void LogVotedFor(int nodeId, int candidateId, int term);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Appended {Count} entries, lastIndex={LastIndex}")]
    private partial void LogEntriesAppended(int count, long lastIndex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Proposed entry at index {Index}, type={CommandType}")]
    private partial void LogEntryProposed(long index, MetadataCommandType commandType);

    [LoggerMessage(Level = LogLevel.Information, Message = "[node {NodeId}] Started election for term {Term}")]
    private partial void LogStartedElection(int nodeId, int term);

    [LoggerMessage(Level = LogLevel.Information, Message = "Waiting for peer discovery before becoming leader (term={Term}, expecting cluster peers)")]
    private partial void LogWaitingForPeerDiscovery(int term);

    [LoggerMessage(Level = LogLevel.Information, Message = "Waiting for peer discovery before enabling elections")]
    private partial void LogWaitingForPeerDiscoveryStart();

    [LoggerMessage(Level = LogLevel.Information, Message = "Peer {PeerId} is reachable, enabling elections")]
    private partial void LogPeerDiscovered(int peerId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Peer {PeerId} was not reached")]
    private partial void LogPeerNotReached(int peerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Peer discovery timeout after {TimeoutSeconds}s, proceeding without peer confirmation")]
    private partial void LogPeerDiscoveryTimeout(int timeoutSeconds);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received vote from {PeerId}, total={Votes}/{Majority}")]
    private partial void LogReceivedVote(int peerId, int votes, int majority);

    // Pre-Vote logging
    [LoggerMessage(Level = LogLevel.Information, Message = "Starting pre-vote for proposed term {ProposedTerm}")]
    private partial void LogStartingPreVote(int proposedTerm);

    [LoggerMessage(Level = LogLevel.Debug, Message = "No campaign for term {ProposedTerm} yet: too few peers have answered the last one for a majority to be asked")]
    private partial void LogCampaignAwaitsAnswers(int proposedTerm);

    [LoggerMessage(Level = LogLevel.Error, Message = "Election safety violated: this node leads term {Term} and node {OtherLeader} claims to lead it too; following it")]
    private partial void LogElectionSafetyViolation(int term, int otherLeader);

    [LoggerMessage(Level = LogLevel.Information, Message = "Pre-vote for term {ProposedTerm} is stale (now term {CurrentTerm}, or a leader was heard from); no election")]
    private partial void LogPreVoteStale(int proposedTerm, int currentTerm);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received pre-vote from {PeerId}, total={PreVotes}/{Majority}")]
    private partial void LogReceivedPreVote(int peerId, int preVotes, int majority);

    [LoggerMessage(Level = LogLevel.Information, Message = "[node {NodeId}] Pre-vote succeeded with {PreVotes}/{Majority} votes, proceeding to election")]
    private partial void LogPreVoteSucceeded(int nodeId, int preVotes, int majority);

    [LoggerMessage(Level = LogLevel.Information, Message = "Pre-vote failed with only {PreVotes}/{Majority} votes, aborting election")]
    private partial void LogPreVoteFailed(int preVotes, int majority);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Pre-vote rejected from {CandidateId}: active leader {LeaderId}")]
    private partial void LogPreVoteRejectedLeaderActive(int candidateId, int leaderId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Pre-vote granted to {CandidateId} for proposed term {ProposedTerm}")]
    private partial void LogPreVoteGranted(int candidateId, int proposedTerm);

    [LoggerMessage(Level = LogLevel.Information, Message = "[node {NodeId}] Became follower in term {Term}")]
    private partial void LogBecameFollower(int nodeId, int term);

    [LoggerMessage(Level = LogLevel.Information, Message = "[node {NodeId}] Became LEADER in term {Term}")]
    private partial void LogBecameLeader(int nodeId, int term);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Commit index advanced to {CommitIndex}")]
    private partial void LogCommitIndexAdvanced(long commitIndex);

    [LoggerMessage(Level = LogLevel.Information, Message = "[node {NodeId}] Caught up with the metadata log at index {Index} in term {Term}")]
    private partial void LogCaughtUp(int nodeId, long index, int term);

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

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in a leadership change handler")]
    private partial void LogLeadershipChangedHandlerError(Exception ex);

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

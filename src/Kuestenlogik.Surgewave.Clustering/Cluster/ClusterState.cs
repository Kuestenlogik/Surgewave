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

    // #60 Inc6a — serializes all writes to _brokers so a read-modify-write (registration merge,
    // level convergence) cannot lose a concurrent update. A dedicated lock (not _stateLock) so broker
    // membership churn never contends with the per-partition state path. Reads stay lock-free on the
    // ConcurrentDictionary. Control-plane only — never taken on the produce/fetch hot path.
    private readonly object _brokerLock = new();

    // #60 Inc6a — serializes a whole controller-push fence-through-apply span across BOTH inter-broker
    // wires. SemaphoreSlim (not a Monitor) so the applier may await BecomeLeader/FollowerAsync inside
    // the critical section without holding a sync lock over I/O. Control-plane only.
    private readonly SemaphoreSlim _controllerPushGate = new(1, 1);

    /// <summary>
    /// Current controller broker ID.
    /// </summary>
    public int ControllerId { get; set; } = -1;

    /// <summary>
    /// Controller epoch (increments on controller election).
    /// </summary>
    public int ControllerEpoch { get; set; }

    /// <summary>
    /// #60 Inc5 — atomically fence-and-advance the controller identity from a controller push.
    /// Returns <c>false</c> (and changes nothing) when <paramref name="controllerEpoch"/> is older
    /// than the current <see cref="ControllerEpoch"/> — the push comes from a demoted controller and
    /// must not be applied. This is the SINGLE fence source for both the Kafka-wire inter-broker
    /// handler and the native applier: with one shared, lock-guarded check, pushes interleaved over
    /// both wires during a rolling upgrade cannot regress the epoch past each other (a per-handler
    /// epoch field could be behind the shared state and let a stale push through).
    /// </summary>
    public bool TryAdvanceControllerEpoch(int controllerId, int controllerEpoch)
    {
        lock (_stateLock)
        {
            if (controllerEpoch < ControllerEpoch)
                return false;

            ControllerEpoch = controllerEpoch;
            ControllerId = controllerId;
            return true;
        }
    }

    /// <summary>
    /// #60 Inc6a — apply a controller-owned partition state (leader / leader-epoch / replicas / ISR)
    /// only when its <paramref name="leaderEpoch"/> is not older than the stored one, atomically under
    /// <c>_stateLock</c>. This is the per-partition ordering fence: a delayed/reordered push carrying
    /// an older leader epoch is skipped (returns <c>false</c>), while an unrelated partition arriving
    /// with any epoch still applies — so disjoint partial pushes never fence each other out. Local
    /// watermarks/log offsets stay follower-owned and are untouched.
    /// </summary>
    public bool TryApplyControllerPartitionState(
        TopicPartition tp, int leaderId, int leaderEpoch, IReadOnlyList<int> replicas, IReadOnlyList<int> isr)
    {
        lock (_stateLock)
        {
            var state = GetOrCreatePartitionState(tp);
            if (leaderEpoch < state.LeaderEpoch)
                return false;

            state.LeaderBrokerId = leaderId;
            state.LeaderEpoch = leaderEpoch;
            state.Replicas.Clear();
            state.Replicas.AddRange(replicas);
            state.Isr.Clear();
            state.Isr.AddRange(isr);
            return true;
        }
    }

    /// <summary>
    /// #60 Inc6a — whether a StopReplica carrying <paramref name="leaderEpoch"/> should be honored for
    /// <paramref name="tp"/>: rejected when the stored partition has a strictly higher leader epoch (a
    /// newer re-assignment supersedes a delayed stop). A leader epoch of <c>-1</c> (v0-2 wire, no
    /// epoch) is always honored, matching the Kafka-wire StopReplica handler.
    /// </summary>
    public bool ShouldStopReplica(TopicPartition tp, int leaderEpoch)
    {
        lock (_stateLock)
        {
            if (leaderEpoch < 0)
                return true;
            return !_partitionStates.TryGetValue(tp, out var state) || leaderEpoch >= state.LeaderEpoch;
        }
    }

    /// <summary>
    /// Election-time controller assumption: atomically increment the epoch and take the controller
    /// id, returning the new epoch. Shares <c>_stateLock</c> with
    /// <see cref="TryAdvanceControllerEpoch"/> so a controller push racing the election cannot
    /// interleave the non-atomic read-increment-write (a lost increment or a demoted ControllerId
    /// paired with the fresh epoch).
    /// </summary>
    public int BecomeController(int controllerId)
    {
        lock (_stateLock)
        {
            ControllerEpoch++;
            ControllerId = controllerId;
            return ControllerEpoch;
        }
    }

    /// <summary>
    /// #60 Inc6a — acquire the exclusive controller-push scope. Both the native and Kafka-wire
    /// inter-broker handlers wrap their whole fence-through-apply span in this scope so two pushes
    /// that both pass the epoch fence cannot interleave their per-partition writes. Dispose the
    /// returned scope (preferably with <c>await using</c>) to release it.
    /// </summary>
    public async ValueTask<IDisposable> AcquireControllerPushScopeAsync(CancellationToken ct = default)
    {
        await _controllerPushGate.WaitAsync(ct).ConfigureAwait(false);
        return new ControllerPushScope(_controllerPushGate);
    }

    private sealed class ControllerPushScope(SemaphoreSlim gate) : IDisposable
    {
        // The scope RELEASES the gate on dispose; it does NOT own or dispose the semaphore (ClusterState
        // does), so CA2213 (dispose the IDisposable field) does not apply.
#pragma warning disable CA2213
        private SemaphoreSlim? _gate = gate;
#pragma warning restore CA2213
        public void Dispose() => Interlocked.Exchange(ref _gate, null)?.Release();
    }

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
    /// #60 Inc3 — the cluster-wide finalized inter-broker protocol level: the MIN of every registered
    /// broker's advertised <see cref="BrokerNode.InterBrokerProtocol"/> (a broker that never advertised
    /// the feature reads as <see cref="InterBrokerProtocolFeature.KafkaWire"/>). Returns
    /// <see cref="InterBrokerProtocolFeature.KafkaWire"/> for an empty cluster.
    /// <para>
    /// This is the safety anchor: the cluster only rises to a higher level once EVERY live or fenced
    /// broker supports it, so a single older peer pins the whole cluster to the Kafka wire. Exposed for
    /// observability — nothing selects transport on it yet (that lands in a later increment).
    /// </para>
    /// </summary>
    public short FinalizedInterBrokerProtocol
    {
        get
        {
            var finalized = short.MaxValue;
            var any = false;

            // Iterate the concurrent map directly via its lock-free enumerator — NOT _brokers.Values,
            // whose getter locks every bucket and copies all values into a fresh List on each access.
            // This is an observability getter, so a moving view of the map is fine; no LINQ, no O(N) copy.
            foreach (var kvp in _brokers)
            {
                any = true;
                var level = kvp.Value.InterBrokerProtocol;
                if (level < finalized)
                    finalized = level;
            }

            return any ? finalized : InterBrokerProtocolFeature.KafkaWire;
        }
    }

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
        lock (_brokerLock)
            _brokers[broker.BrokerId] = broker;
    }

    /// <summary>
    /// Atomically add-or-update a broker: adds <paramref name="ifAbsent"/> when the broker is unknown,
    /// otherwise applies <paramref name="mutate"/> to the current node — the read-modify-write runs
    /// under <c>_brokerLock</c>, which every broker mutator holds, so a concurrent registration or
    /// convergence cannot lose an update. Use instead of <see cref="GetBroker"/> + <see cref="AddBroker"/>
    /// for registration merges and level convergence (#60 Inc6a). Returns the stored node.
    /// <para>
    /// A CAS on the ConcurrentDictionary would be wrong here: <see cref="BrokerNode"/>'s equality is
    /// BrokerId-only (so it can be a dictionary key), which makes <c>TryUpdate</c>'s value comparison
    /// always match and defeats the compare-and-swap — hence the explicit lock.
    /// </para>
    /// </summary>
    public BrokerNode UpdateBroker(int brokerId, BrokerNode ifAbsent, Func<BrokerNode, BrokerNode> mutate)
        => UpdateBroker(brokerId, ifAbsent, mutate, out _);

    /// <summary>
    /// As <see cref="UpdateBroker(int,BrokerNode,Func{BrokerNode,BrokerNode})"/>, additionally
    /// reporting via <paramref name="inserted"/> whether the broker was newly added (true) or an
    /// existing node was mutated (false) — evaluated INSIDE the lock so callers can log accurately
    /// without a separate racy pre-read.
    /// </summary>
    public BrokerNode UpdateBroker(int brokerId, BrokerNode ifAbsent, Func<BrokerNode, BrokerNode> mutate, out bool inserted)
    {
        lock (_brokerLock)
        {
            var present = _brokers.TryGetValue(brokerId, out var current);
            inserted = !present;
            var next = present ? mutate(current!) : ifAbsent;
            _brokers[brokerId] = next;
            return next;
        }
    }

    /// <summary>
    /// Register or update a broker with the given ID, host, and port.
    /// </summary>
    public void RegisterBroker(int brokerId, string host, int port, string? rack = null)
    {
        lock (_brokerLock)
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
        lock (_brokerLock)
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

    /// <summary>
    /// Resolve a topic by its unique ID. Used by the controller to map the
    /// TopicId carried in flexible inter-broker requests (e.g. AlterPartition
    /// v2+) back to a topic name. Returns <c>null</c> if unknown.
    /// </summary>
    public TopicMetadata? GetTopicById(Guid topicId)
    {
        if (topicId == Guid.Empty)
            return null;

        foreach (var topic in _topics.Values)
        {
            if (topic.TopicId == topicId)
                return topic;
        }

        return null;
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
    /// Deep-copy a partition state under <c>_stateLock</c> so a concurrent ISR/replica mutation
    /// cannot tear the copied lists (mirrors <see cref="GetIsrSnapshot"/>). Used by the native
    /// controller client before its two-pass frame encode (#60 Inc5): the source lists are the live
    /// shared ones, and a plain <c>[.. list]</c> copy racing a Clear+AddRange can throw or observe a
    /// partial list.
    /// </summary>
    public PartitionState CopyPartitionStateLocked(PartitionState state)
    {
        lock (_stateLock)
        {
            return new PartitionState
            {
                TopicPartition = state.TopicPartition,
                LeaderBrokerId = state.LeaderBrokerId,
                LeaderEpoch = state.LeaderEpoch,
                Replicas = [.. state.Replicas],
                Isr = [.. state.Isr],
                OfflineReplicas = [.. state.OfflineReplicas],
                MinInSyncReplicas = state.MinInSyncReplicas,
                HighWatermark = state.HighWatermark,
                LogStartOffset = state.LogStartOffset,
            };
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
    /// Return a point-in-time copy of a partition's ISR, taken under the state
    /// lock so callers never observe a torn list mid-mutation. Returns an empty
    /// list for an unknown partition.
    /// </summary>
    public List<int> GetIsrSnapshot(TopicPartition tp)
    {
        lock (_stateLock)
        {
            return _partitionStates.TryGetValue(tp, out var state)
                ? [.. state.Isr]
                : [];
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
            // Nesting order is always _stateLock -> _brokerLock (no mutator takes _brokerLock then
            // _stateLock), so this cannot deadlock the broker-write path.
            lock (_brokerLock)
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

using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// What a broker knows of its cluster, for the answers that describe it: the controller, the brokers, and who leads
/// and replicates a partition (#272).
/// </summary>
/// <remarks>
/// The native cluster and topic answers and the gRPC metadata were written for a single broker and named the broker
/// asked as the controller, the only node and the only replica of every partition, in a cluster of three as well.
/// They answer from this, as the Kafka Metadata path answers from the cluster state. A broker that runs alone has no
/// view, and answers as before.
/// </remarks>
public sealed class ClusterView
{
    private readonly ClusterState _state;
    private readonly Func<int, bool> _isAlive;
    private readonly Func<int> _raftTerm;

    /// <param name="state">The cluster state the metadata log is applied to.</param>
    /// <param name="localBrokerId">This broker.</param>
    /// <param name="isAlive">Whether this broker holds a broker to be alive: its failure detector.</param>
    /// <param name="raftTerm">The term this broker's Raft node is in.</param>
    public ClusterView(ClusterState state, int localBrokerId, Func<int, bool> isAlive, Func<int> raftTerm)
    {
        _state = state;
        LocalBrokerId = localBrokerId;
        _isAlive = isAlive;
        _raftTerm = raftTerm;
    }

    /// <summary>This broker.</summary>
    public int LocalBrokerId { get; }

    /// <summary>The controller this broker knows, -1 while it knows none.</summary>
    public int ControllerId => _state.ControllerId;

    /// <summary>Whether this broker is the controller.</summary>
    public bool IsController => ControllerId == LocalBrokerId;

    /// <summary>The controller's epoch.</summary>
    public int ControllerEpoch => _state.ControllerEpoch;

    /// <summary>The term this broker's Raft node is in.</summary>
    public int RaftTerm => _raftTerm();

    /// <summary>The topics of the cluster, not only those this broker holds a replica of.</summary>
    public IReadOnlyCollection<TopicMetadata> Topics => [.. _state.Topics.Values];

    /// <summary>The brokers of the cluster, by id.</summary>
    public IReadOnlyList<BrokerView> Brokers =>
        [.. _state.Brokers.Values
            .OrderBy(b => b.BrokerId)
            .Select(b => new BrokerView(b, IsController: b.BrokerId == ControllerId, IsAlive: _isAlive(b.BrokerId)))];

    /// <summary>The topic as the cluster knows it, null when it does not.</summary>
    public TopicMetadata? Topic(string name) => _state.GetTopic(name);

    /// <summary>
    /// Who leads and replicates the partition, null when the cluster state does not know it. The leader is -1 for a
    /// partition without one.
    /// </summary>
    public PartitionView? Partition(TopicPartition partition)
    {
        var state = _state.GetPartitionState(partition);
        return state is null
            ? null
            : new PartitionView(state.LeaderBrokerId, state.LeaderEpoch, [.. state.Replicas], _state.GetIsrSnapshot(partition));
    }
}

/// <summary>A broker of the cluster as another broker sees it.</summary>
public readonly record struct BrokerView(BrokerNode Broker, bool IsController, bool IsAlive);

/// <summary>A partition's leader, replicas and in-sync replicas.</summary>
public readonly record struct PartitionView(int Leader, int LeaderEpoch, IReadOnlyList<int> Replicas, IReadOnlyList<int> Isr);

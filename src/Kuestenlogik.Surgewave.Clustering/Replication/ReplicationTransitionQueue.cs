using System.Threading.Channels;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Carries out the replication transitions a committed metadata entry implies, in log
/// order and off the Raft apply loop (#165).
/// </summary>
/// <remarks>
/// <para>
/// A leadership change has two effects on every broker: knowing who leads the partition,
/// and acting on it — taking over as leader, or starting to fetch from the new one. The
/// controller push does both. The metadata log did only the first: the state machine wrote
/// cluster state and stopped, so the log knew and nobody acted.
/// </para>
/// <para>
/// Off the apply loop because <see cref="Raft.IRaftStateMachine.Apply"/> is synchronous and
/// runs inside it. Awaiting a follower's transition there would block consensus behind an
/// I/O-bound operation — a slow disk on one broker would stall commits for the cluster. The
/// push path runs these on a request thread for the same reason.
/// </para>
/// <para>
/// One consumer, so transitions happen in the order the log committed them. Two workers
/// could apply epoch 7 after epoch 8 and leave a partition following a leader that has
/// already been replaced; the per-transition epoch check makes that recoverable rather than
/// permanent, but ordering keeps it from arising.
/// </para>
/// <para>
/// Unbounded on purpose. A bounded channel would have to either block the apply loop — the
/// thing this exists to avoid — or drop transitions, and a dropped one is a partition that
/// silently never starts replicating. Metadata changes are rare relative to data, and a
/// backlog here means the broker is already failing to keep up in a way that dropping would
/// hide rather than fix.
/// </para>
/// </remarks>
public sealed class ReplicationTransitionQueue : IAsyncDisposable
{
    private readonly ReplicaManager _replicaManager;
    private readonly int _localBrokerId;
    private readonly ILogger<ReplicationTransitionQueue> _logger;
    private readonly Channel<PendingTransition> _channel =
        Channel.CreateUnbounded<PendingTransition>(new UnboundedChannelOptions { SingleReader = true });
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _worker;

    public ReplicationTransitionQueue(
        ReplicaManager replicaManager,
        int localBrokerId,
        ILogger<ReplicationTransitionQueue> logger)
    {
        _replicaManager = replicaManager;
        _localBrokerId = localBrokerId;
        _logger = logger;
        _worker = Task.Run(() => RunAsync(_cts.Token));
    }

    /// <summary>
    /// Queues the transition a committed leadership change implies for this broker, if any.
    /// </summary>
    /// <remarks>
    /// Returns without queueing when this broker is neither the new leader nor a replica —
    /// the partition is simply none of its business, which is the common case in a cluster
    /// of any size and must not cost a queue entry per broker per change.
    /// </remarks>
    public void EnqueueLeadershipChange(TopicPartition partition, int leaderBrokerId, int leaderEpoch, IReadOnlyList<int> replicas)
    {
        if (Route(partition, leaderBrokerId, leaderEpoch, replicas, _localBrokerId) is { } transition)
            _channel.Writer.TryWrite(transition);
    }

    /// <summary>
    /// Decides which transition a leadership change implies for a given broker, or none.
    /// </summary>
    /// <remarks>
    /// Separated from the queueing so the rule can be asserted directly. Testing it through
    /// the queue would mean either running a replica manager against real storage, or a
    /// stand-in that restates the rule — and a test that restates the rule tests itself.
    /// </remarks>
    internal static PendingTransition? Route(
        TopicPartition partition, int leaderBrokerId, int leaderEpoch, IReadOnlyList<int> replicas, int localBrokerId)
    {
        if (leaderBrokerId == localBrokerId)
            return new PendingTransition(partition, LeaderBrokerId: -1, leaderEpoch);

        // Not the leader and not a replica: the partition is none of this broker's business,
        // which is most of them in a cluster of any size.
        return replicas.Contains(localBrokerId)
            ? new PendingTransition(partition, leaderBrokerId, leaderEpoch)
            : null;
    }

    private async Task RunAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var pending in _channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    if (pending.LeaderBrokerId < 0)
                    {
                        await _replicaManager.BecomeLeaderAsync(pending.Partition, pending.LeaderEpoch, ct)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await _replicaManager.BecomeFollowerAsync(
                            pending.Partition, pending.LeaderBrokerId, pending.LeaderEpoch, ct).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // Per transition, as the push path is per partition: one partition that
                    // cannot transition must not stop the rest. The controller re-sends the
                    // state on the next change, and the broker's own fetch loop retries.
                    _logger.LogError(ex,
                        "Replication transition failed for {Topic}-{Partition} (leader {Leader}, epoch {Epoch})",
                        pending.Partition.Topic, pending.Partition.Partition, pending.LeaderBrokerId, pending.LeaderEpoch);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown.
        }
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        await _cts.CancelAsync().ConfigureAwait(false);

        try { await _worker.ConfigureAwait(false); }
        catch (OperationCanceledException) { /* expected */ }

        _cts.Dispose();
    }

    /// <param name="LeaderBrokerId">-1 means "we are the leader"; otherwise the leader to follow.</param>
    internal readonly record struct PendingTransition(TopicPartition Partition, int LeaderBrokerId, int LeaderEpoch);
}

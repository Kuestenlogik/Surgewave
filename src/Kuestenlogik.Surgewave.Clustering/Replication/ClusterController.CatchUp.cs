using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

public sealed partial class ClusterController
{
    private ReplicationTransitionQueue? _transitions;

    // Completed when this broker has caught up with the metadata log, cancelled when the controller stops first.
    private readonly TaskCompletionSource _metadataCaughtUp = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Sets the queue that carries out what the metadata log implies for this broker's replicas, so that
    /// <see cref="WaitForMetadataCaughtUpAsync"/> also waits for the transitions of the entries applied while
    /// catching up. Absent it, the wait ends when the entries are applied.
    /// </summary>
    public void SetReplicationTransitions(ReplicationTransitionQueue transitions) => _transitions = transitions;

    /// <summary>
    /// Whether this broker has caught up with the metadata log since it started; see
    /// <see cref="WaitForMetadataCaughtUpAsync"/>.
    /// </summary>
    public bool IsMetadataCaughtUp => _metadataCaughtUp.Task.IsCompletedSuccessfully;

    /// <summary>
    /// The startup handshake: completes when this broker has caught up with the metadata log, at once if it already
    /// has. Cancelled with <paramref name="ct"/>, and when the controller stops before the broker got there.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A broker that has just started holds its metadata log and has applied none of it: it knows no topic, no
    /// config and no controller until its Raft node leads or follows a leader, and answers a client from that state
    /// if it lets one in. A host therefore waits for this before it accepts clients and before it reports itself
    /// ready — as a Kafka broker in KRaft mode starts processing requests only after it has caught up with the
    /// metadata log.
    /// </para>
    /// <para>
    /// Caught up is what <see cref="Raft.RaftNode.WaitForCaughtUpAsync"/> says — on a broker that is its whole quorum:
    /// elected, and its own log applied; in a cluster: applied what the leader of the current term had committed —
    /// and, with <see cref="SetReplicationTransitions"/>, the replica transitions of those entries carried out, so
    /// the broker also leads and follows the partitions the log gives it.
    /// </para>
    /// <para>
    /// Not bounded. A broker that cannot reach a quorum has no metadata to answer from and stays not ready for as
    /// long as that lasts; its Raft node logs every election it loses.
    /// </para>
    /// </remarks>
    public Task WaitForMetadataCaughtUpAsync(CancellationToken ct) => _metadataCaughtUp.Task.WaitAsync(ct);

    // Started with the controller, under its lifetime.
    private async Task CatchUpWithMetadataAsync(Raft.RaftNode raftNode, CancellationToken ct)
    {
        try
        {
            await raftNode.WaitForCaughtUpAsync(ct).ConfigureAwait(false);
            if (_transitions is { } transitions)
                await transitions.WhenCarriedOutAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The controller, the node or the queue stopped first.
            _metadataCaughtUp.TrySetCanceled(CancellationToken.None);
            return;
        }

        // Said before it is signalled, so that it precedes in the log whatever a waiter says next — the host's ready
        // line, for one.
        LogMetadataCaughtUp(_config.BrokerId, raftNode.CommitIndex);
        _metadataCaughtUp.TrySetResult();
    }

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Broker {BrokerId} has caught up with the metadata log (commit index {CommitIndex})")]
    private partial void LogMetadataCaughtUp(int brokerId, long commitIndex);
}

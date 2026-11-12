using Kuestenlogik.Surgewave.Clustering.Raft;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

public sealed partial class ClusterController
{
    // What this controller writes to the metadata log passes through here, and DisposeAsync closes it.
    private readonly WriteGate _decisions = new();

    /// <summary>
    /// Proposes an entry to the metadata log: every write of this controller. Returns the entry's index, or -1 when
    /// the node does not lead or this controller has stopped.
    /// </summary>
    /// <remarks>
    /// A stopped controller decides nothing. Its work is cancelled by the stop, but a cancellation ends a wait, not a
    /// write: the controller of a broker that had just taken office went on to register itself and to create the
    /// internal topics after <see cref="DisposeAsync"/> had returned, on a node that still led or — the hosts dispose
    /// the node first — on one already disposed.
    /// </remarks>
    private async Task<long> ProposeAsync(MetadataCommandType commandType, byte[] data, CancellationToken ct)
    {
        if (_raftNode is not { } raftNode || !_decisions.TryEnter())
            return -1;

        try
        {
            return await raftNode.ProposeAsync(commandType, data, ct).ConfigureAwait(false);
        }
        finally
        {
            _decisions.Exit();
        }
    }
}

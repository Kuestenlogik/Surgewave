namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// The voter set Surgewave has today: every broker the transport knows about, plus this one
/// (#166).
/// </summary>
/// <remarks>
/// <para>
/// This is Kafka's <c>combined mode</c> — every node is both broker and controller — and it
/// is a supported shape for a small cluster. It stops being one as the cluster grows,
/// because the quorum grows with it: a metadata write then waits for a majority of *all*
/// brokers, and the latency is that of the slowest node in that majority.
/// </para>
/// <para>
/// Kept as the default so this seam changes nothing on its own. An explicit controller
/// quorum replaces it rather than modifying it.
/// </para>
/// </remarks>
public sealed class TransportDerivedVoterSet : IRaftVoterSet
{
    private readonly IRaftTransport _transport;
    private readonly int _localNodeId;

    // The peer list is re-read on every heartbeat, so the derived list is cached against
    // the transport's answer and rebuilt only when that changes. Without it every
    // heartbeat interval would allocate a fresh list per call site.
    private IReadOnlyList<int>? _lastPeers;
    private IReadOnlyList<int> _voterIds = [];

    public TransportDerivedVoterSet(IRaftTransport transport, int localNodeId)
    {
        _transport = transport;
        _localNodeId = localNodeId;
    }

    /// <inheritdoc />
    public IReadOnlyList<int> VoterIds
    {
        get
        {
            var peers = _transport.GetPeerIds();
            if (!ReferenceEquals(peers, _lastPeers) && !SameIds(peers, _voterIds))
            {
                _lastPeers = peers;
                var voters = new List<int>(peers.Count + 1) { _localNodeId };
                voters.AddRange(peers.Where(id => id != _localNodeId));
                _voterIds = voters;
            }

            return _voterIds;
        }
    }

    /// <inheritdoc />
    public int Majority => (VoterIds.Count / 2) + 1;

    /// <summary>
    /// Whether the transport's peers are already reflected in the cached voters. Compares
    /// against the cached list minus self, so a peer joining or leaving invalidates.
    /// </summary>
    private bool SameIds(IReadOnlyList<int> peers, IReadOnlyList<int> voters)
    {
        if (voters.Count != peers.Count + 1) return false;

        for (var i = 0; i < peers.Count; i++)
        {
            // voters[0] is self; peers keep their order after it.
            if (voters[i + 1] != peers[i]) return false;
        }

        return true;
    }
}

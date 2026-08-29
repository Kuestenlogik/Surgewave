namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// The voter set an operator stated with <c>controller.quorum.voters</c> (#168).
/// </summary>
/// <remarks>
/// <para>
/// Fixed for the lifetime of the node, which is the whole point: the transport-derived set it
/// replaces grows with the cluster, so a metadata write waits for a majority of every broker
/// and gets slower as brokers are added. This one stays the size the operator chose.
/// </para>
/// <para>
/// Every node the transport can reach that is not named here becomes an observer — it
/// receives the metadata log without voting on it (#167).
/// </para>
/// <para>
/// Changing the set at runtime is KIP-853's online reconfiguration and is not implemented;
/// the list is read once at startup.
/// </para>
/// </remarks>
public sealed class ConfiguredVoterSet : IRaftVoterSet
{
    public ConfiguredVoterSet(IEnumerable<int> voterIds)
    {
        VoterIds = voterIds.Distinct().ToArray();
    }

    /// <inheritdoc />
    public IReadOnlyList<int> VoterIds { get; }

    /// <inheritdoc />
    public int Majority => (VoterIds.Count / 2) + 1;
}

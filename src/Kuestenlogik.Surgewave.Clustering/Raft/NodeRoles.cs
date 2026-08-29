namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// What a node does in the cluster — Kafka's <c>process.roles</c> (#168).
/// </summary>
/// <remarks>
/// <para>
/// A node that is both is running in <em>combined mode</em>: it serves clients and takes part
/// in the metadata quorum. That is what every Surgewave node did before this existed, and it
/// stays the default, because it is also the right shape for a single broker and for an
/// embedded host.
/// </para>
/// <para>
/// Separating the roles is what makes the quorum smaller than the cluster. A metadata write
/// then waits for a majority of the controllers rather than a majority of every broker, so
/// adding brokers stops making metadata slower.
/// </para>
/// </remarks>
[Flags]
public enum NodeRoles
{
    /// <summary>No role — never a valid configuration, only the parse failure value.</summary>
    None = 0,

    /// <summary>Serves clients: produce, fetch, and the partitions this node holds.</summary>
    Broker = 1,

    /// <summary>Takes part in the metadata quorum: votes, and can lead it.</summary>
    Controller = 2,
}

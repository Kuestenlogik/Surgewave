using System.Collections.Immutable;

namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Immutable snapshot of the Raft cluster's voter set. Surgewave currently builds
/// one of these from the static broker config at startup; a future KIP-853
/// implementation will mutate it via <see cref="MetadataCommandType.VoterChange"/>
/// log entries replicated through the standard log machinery. Modelling the
/// voter set as a first-class type now lets future code construct a new
/// configuration by calling <see cref="AddVoter"/> / <see cref="RemoveVoter"/>
/// / <see cref="UpdateVoter"/> and serialise the result without touching every
/// caller.
/// </summary>
public sealed record RaftConfiguration
{
    /// <summary>The voters that participate in this configuration.</summary>
    public required ImmutableArray<RaftVoter> Voters { get; init; }

    /// <summary>
    /// Sequence number that monotonically increases with every voter-set
    /// change. Used by the single-server-change algorithm to enforce the
    /// "only one diff in flight" invariant — the leader rejects a new
    /// voter-change while the previous one has not yet committed under
    /// both the old and new majority.
    /// </summary>
    public required long ConfigurationSequence { get; init; }

    /// <summary>
    /// Number of voters required to make decisions in this configuration.
    /// Always <c>(Voters.Length / 2) + 1</c> for the strict-majority rule.
    /// </summary>
    public int Majority => (Voters.Length / 2) + 1;

    /// <summary>
    /// Returns a fresh configuration with <paramref name="voter"/> added.
    /// Throws when the voter id is already present — KIP-853's single-server
    /// change forbids "no-op" changes; if the caller wants to refresh
    /// listeners they go through <see cref="UpdateVoter"/>.
    /// </summary>
    public RaftConfiguration AddVoter(RaftVoter voter)
    {
        ArgumentNullException.ThrowIfNull(voter);
        if (Voters.Any(v => v.NodeId == voter.NodeId))
        {
            throw new InvalidOperationException(
                $"Voter {voter.NodeId} is already in the configuration; use UpdateVoter to change its listeners.");
        }
        return new RaftConfiguration
        {
            Voters = Voters.Add(voter),
            ConfigurationSequence = ConfigurationSequence + 1,
        };
    }

    /// <summary>
    /// Returns a fresh configuration with <paramref name="nodeId"/> removed.
    /// Throws when the voter is not present (defensive — silently no-op'ing
    /// would mask a programming error in the caller).
    /// </summary>
    public RaftConfiguration RemoveVoter(int nodeId)
    {
        var existing = Voters.FirstOrDefault(v => v.NodeId == nodeId);
        if (existing is null)
        {
            throw new InvalidOperationException(
                $"Voter {nodeId} is not in the configuration.");
        }
        return new RaftConfiguration
        {
            Voters = Voters.Remove(existing),
            ConfigurationSequence = ConfigurationSequence + 1,
        };
    }

    /// <summary>
    /// Returns a fresh configuration with <paramref name="voter"/> replacing
    /// the existing entry with the same node id. Throws when no entry with
    /// that id is present.
    /// </summary>
    public RaftConfiguration UpdateVoter(RaftVoter voter)
    {
        ArgumentNullException.ThrowIfNull(voter);
        var existing = Voters.FirstOrDefault(v => v.NodeId == voter.NodeId);
        if (existing is null)
        {
            throw new InvalidOperationException(
                $"Voter {voter.NodeId} is not in the configuration.");
        }
        return new RaftConfiguration
        {
            Voters = Voters.Replace(existing, voter),
            ConfigurationSequence = ConfigurationSequence + 1,
        };
    }

    /// <summary>True if a voter with the given node id is in this configuration.</summary>
    public bool ContainsVoter(int nodeId) => Voters.Any(v => v.NodeId == nodeId);

    /// <summary>The empty / bootstrap configuration — never written to disk.</summary>
    public static RaftConfiguration Empty { get; } = new()
    {
        Voters = ImmutableArray<RaftVoter>.Empty,
        ConfigurationSequence = 0,
    };
}

/// <summary>
/// One participant in a <see cref="RaftConfiguration"/>. Captures the
/// minimum a future single-server-change implementation needs: a stable
/// node id, the directory id from the storage layer (used by KIP-853 to
/// distinguish a freshly-formatted broker from a returning one with the
/// same id), and the listener endpoints peers can talk to.
/// </summary>
public sealed record RaftVoter
{
    /// <summary>Stable broker id of this voter.</summary>
    public required int NodeId { get; init; }

    /// <summary>
    /// Directory id from the broker's storage layer. KIP-853 carries this
    /// alongside the node id so the cluster can detect a "wiped + restarted"
    /// node that has lost its log and would otherwise vote with stale state.
    /// </summary>
    public required Guid DirectoryId { get; init; }

    /// <summary>Listener endpoints the voter exposes for inter-broker traffic.</summary>
    public required ImmutableArray<RaftListener> Listeners { get; init; }
}

/// <summary>
/// One endpoint in a voter's listener set. <see cref="Name"/> matches the
/// Kafka <c>listeners</c> property (e.g. <c>CONTROLLER</c>, <c>INTERNAL</c>);
/// peers pick the listener whose name matches their own <c>inter.broker.listener.name</c>.
/// </summary>
public sealed record RaftListener
{
    public required string Name { get; init; }
    public required string Host { get; init; }
    public required ushort Port { get; init; }
}

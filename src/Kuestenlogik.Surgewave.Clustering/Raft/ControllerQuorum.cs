namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Reads and checks Kafka's <c>controller.quorum.voters</c> (#168).
/// </summary>
/// <remarks>
/// <para>
/// The voter list and <see cref="ClusteringConfig.ClusterNodes"/> answer different questions
/// and neither derives from the other: cluster nodes say who exists, the voter list says who
/// decides. That is the same split <see cref="RaftNode"/> makes between its transport and its
/// voter set, and keeping it in the configuration is what lets an operator add brokers
/// without enlarging the quorum.
/// </para>
/// <para>
/// They still must not contradict each other, so the checks here are about agreement rather
/// than derivation — a voter nobody has heard of is a typo, and a typo in this list is a
/// cluster that never forms a quorum.
/// </para>
/// </remarks>
public static class ControllerQuorum
{
    /// <summary>
    /// Parses the configured voter list, appending one error per entry it cannot read.
    /// </summary>
    /// <returns>The voters that parsed; empty when nothing was configured.</returns>
    public static IReadOnlyList<ControllerQuorumVoter> Parse(string configured, ICollection<string> errors)
    {
        if (string.IsNullOrWhiteSpace(configured))
            return [];

        var voters = new List<ControllerQuorumVoter>();
        foreach (var entry in configured.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            if (ControllerQuorumVoter.TryParse(entry, out var voter, out var error))
                voters.Add(voter);
            else
                errors.Add($"{nameof(ClusteringConfig.ControllerQuorumVoters)}: {error}");
        }

        return voters;
    }

    /// <summary>
    /// Checks the roles and the voter list against each other and against the known cluster.
    /// </summary>
    public static void Validate(ClusteringConfig config, ICollection<string> errors)
    {
        if (!NodeRolesParser.TryParse(config.ProcessRoles, out var roles, out var rolesError))
        {
            errors.Add($"{nameof(ClusteringConfig.ProcessRoles)}: {rolesError}");
            return;
        }

        var voters = Parse(config.ControllerQuorumVoters, errors);
        if (voters.Count == 0)
        {
            // No explicit quorum: every node votes, which is what this cluster did before the
            // setting existed. A node cannot then be a broker only — there would be nobody
            // left to decide.
            if (!roles.HasFlag(NodeRoles.Controller))
            {
                errors.Add(
                    $"{nameof(ClusteringConfig.ProcessRoles)} excludes 'controller' but "
                    + $"{nameof(ClusteringConfig.ControllerQuorumVoters)} is empty; a node can only drop the "
                    + "controller role once the quorum names who keeps it.");
            }

            return;
        }

        var duplicate = voters.GroupBy(v => v.NodeId).FirstOrDefault(g => g.Count() > 1);
        if (duplicate is not null)
        {
            errors.Add(
                $"{nameof(ClusteringConfig.ControllerQuorumVoters)}: node {duplicate.Key} appears more than once; "
                + "a voter counts towards the majority once, so the duplicate would silently shrink the quorum.");
        }

        var listed = voters.Any(v => v.NodeId == config.BrokerId);

        if (roles.HasFlag(NodeRoles.Controller) && !listed)
        {
            errors.Add(
                $"{nameof(ClusteringConfig.ProcessRoles)} includes 'controller' but this broker "
                + $"({config.BrokerId}) is not in {nameof(ClusteringConfig.ControllerQuorumVoters)}; a controller "
                + "outside its own quorum would campaign for a vote it does not have.");
        }

        if (!roles.HasFlag(NodeRoles.Controller) && listed)
        {
            errors.Add(
                $"This broker ({config.BrokerId}) is in {nameof(ClusteringConfig.ControllerQuorumVoters)} but "
                + $"{nameof(ClusteringConfig.ProcessRoles)} excludes 'controller'; the rest of the quorum would "
                + "wait for a vote this node never casts.");
        }

        ValidateAgainstClusterNodes(config, voters, errors);
    }

    /// <summary>
    /// Whether an even voter count was configured — legal, but a worse choice than the odd
    /// number either side of it.
    /// </summary>
    /// <remarks>
    /// Four voters need three to agree, exactly as five do, so the fourth adds a node that can
    /// fail without adding any failure it can survive. Reported as a warning rather than an
    /// error because it is a poor choice, not a broken one — and during a rolling change from
    /// three voters to five, an even count is a legitimate intermediate state.
    /// </remarks>
    public static bool IsEvenVoterCount(IReadOnlyList<ControllerQuorumVoter> voters)
        => voters.Count > 0 && voters.Count % 2 == 0;

    private static void ValidateAgainstClusterNodes(
        ClusteringConfig config, IReadOnlyList<ControllerQuorumVoter> voters, ICollection<string> errors)
    {
        if (string.IsNullOrWhiteSpace(config.ClusterNodes))
            return;

        // ClusterNodes is "brokerId:host:port[:replicationPort]", parsed the same way the
        // controller parses it. Only the ids matter here.
        var known = new HashSet<int> { config.BrokerId };
        foreach (var node in config.ClusterNodes.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = node.Trim().Split(':');
            if (parts.Length >= 3 && int.TryParse(parts[0], out var brokerId))
                known.Add(brokerId);
        }

        foreach (var voter in voters)
        {
            if (!known.Contains(voter.NodeId))
            {
                errors.Add(
                    $"{nameof(ClusteringConfig.ControllerQuorumVoters)}: node {voter.NodeId} is not in "
                    + $"{nameof(ClusteringConfig.ClusterNodes)}; a voter no broker knows about is counted towards "
                    + "the majority and can never answer, so the quorum would be short by one from the start.");
            }
        }
    }
}

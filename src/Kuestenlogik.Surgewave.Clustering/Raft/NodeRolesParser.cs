namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Reads Kafka's <c>process.roles</c> spelling into <see cref="NodeRoles"/> (#168).
/// </summary>
public static class NodeRolesParser
{
    /// <summary>The default: both roles, which is combined mode.</summary>
    public const string CombinedMode = "broker,controller";

    /// <summary>
    /// Parses a comma-separated role list, or explains why it is not one.
    /// </summary>
    /// <remarks>
    /// An unknown role is an error rather than something to ignore. Silently dropping
    /// <c>contoller</c> would leave the node a plain broker, and the cluster would then fail
    /// to form a quorum for a reason nothing in the logs connects to a typo.
    /// </remarks>
    public static bool TryParse(string configured, out NodeRoles roles, out string? error)
    {
        roles = NodeRoles.None;
        error = null;

        foreach (var part in configured.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            switch (part.ToLowerInvariant())
            {
                case "broker":
                    roles |= NodeRoles.Broker;
                    break;
                case "controller":
                    roles |= NodeRoles.Controller;
                    break;
                default:
                    error = $"'{part}' is not a role; the values are 'broker' and 'controller'.";
                    return false;
            }
        }

        if (roles == NodeRoles.None)
        {
            error = "at least one role is required; the values are 'broker' and 'controller'.";
            return false;
        }

        return true;
    }
}

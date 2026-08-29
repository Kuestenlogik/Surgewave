namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// One entry of Kafka's <c>controller.quorum.voters</c>: <c>id@host:port</c> (#168).
/// </summary>
/// <remarks>
/// Deliberately not <see cref="RaftVoter"/>, which additionally requires a directory id and a
/// named listener set. The wire format carries neither, so building one here would mean
/// inventing a directory id — fake data in the type whose entire purpose is to be
/// authoritative about voter identity. This record says exactly what the operator wrote.
/// </remarks>
/// <param name="NodeId">The voter's stable broker id.</param>
/// <param name="Host">Host the voter is reachable at.</param>
/// <param name="Port">Port the voter is reachable at.</param>
public readonly record struct ControllerQuorumVoter(int NodeId, string Host, int Port)
{
    /// <summary>
    /// Parses one <c>id@host:port</c> entry, or explains why it is not one.
    /// </summary>
    /// <remarks>
    /// The error text names the entry as written. A quorum list is typed by hand into a
    /// deployment file and usually read again only after the cluster fails to form, so it has
    /// to say which entry is wrong rather than that something is.
    /// </remarks>
    public static bool TryParse(string entry, out ControllerQuorumVoter voter, out string? error)
    {
        voter = default;
        error = null;

        var trimmed = entry.Trim();
        var at = trimmed.IndexOf('@');
        if (at <= 0 || at == trimmed.Length - 1)
        {
            error = $"'{trimmed}' is not a voter; the form is id@host:port.";
            return false;
        }

        if (!int.TryParse(trimmed[..at], out var nodeId) || nodeId < 0)
        {
            error = $"'{trimmed}' has no valid node id before the '@'.";
            return false;
        }

        var endpoint = trimmed[(at + 1)..];

        // Last colon, so an IPv6 literal in brackets keeps its own colons.
        var colon = endpoint.LastIndexOf(':');
        if (colon <= 0 || colon == endpoint.Length - 1)
        {
            error = $"'{trimmed}' has no host:port after the '@'.";
            return false;
        }

        if (!int.TryParse(endpoint[(colon + 1)..], out var port) || port is < 1 or > 65535)
        {
            error = $"'{trimmed}' has no valid port; it must be between 1 and 65535.";
            return false;
        }

        voter = new ControllerQuorumVoter(nodeId, endpoint[..colon], port);
        return true;
    }
}

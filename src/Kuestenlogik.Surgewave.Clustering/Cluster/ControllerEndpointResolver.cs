using Kuestenlogik.Surgewave.Clustering.Raft;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Decides which endpoint a broker talks to when it wants the controller (#169).
/// </summary>
/// <remarks>
/// <para>
/// A voter can work this out from having been part of the quorum. An observer cannot: it is
/// outside the quorum by construction, so it has to be told where the quorum is before it can
/// replicate, and it has nothing but the configuration to be told by.
/// </para>
/// <para>
/// Three sources, in order of how much they know:
/// </para>
/// <list type="number">
/// <item>The controller cluster state names — which in Raft mode is the current leader, so a
/// leadership change is followed without going back to the configuration.</item>
/// <item>The configured quorum, tried in turn. This is first contact, and it is also the
/// recovery path when the node cluster state names has stopped answering.</item>
/// <item>The lowest-id known broker — the convention combined mode has always used, and still
/// the right answer when no quorum was configured.</item>
/// </list>
/// <para>
/// The configured list only has to be good enough to reach the quorum once. After that the log
/// carries the cluster's own view, which is what makes a list that has gone stale survivable
/// rather than fatal.
/// </para>
/// </remarks>
public sealed class ControllerEndpointResolver
{
    /// <summary>
    /// Consecutive failures after which the controller from cluster state stops being trusted.
    /// </summary>
    /// <remarks>
    /// Without this, a controller that is named but not answering is returned forever and the
    /// other voters are never tried — the node waits for one machine while a quorum it could
    /// reach is sitting there. Two, because one failure is a dropped connection and three would
    /// spend another interval on a node that has already stopped twice.
    /// </remarks>
    private const int FailuresBeforeRotating = 2;

    private readonly ClusteringConfig _config;
    private readonly ClusterState _clusterState;
    private readonly IReadOnlyList<ControllerQuorumVoter> _quorum;

    private int _consecutiveFailures;
    private int _rotation;
    private bool _everSucceeded;

    public ControllerEndpointResolver(ClusteringConfig config, ClusterState clusterState)
    {
        _config = config;
        _clusterState = clusterState;

        // Parse errors are reported where the voter set is built and by the config validator;
        // an entry that cannot be read is simply not a candidate here.
        _quorum = ControllerQuorum.Parse(config.ControllerQuorumVoters, [])
            .Where(v => v.NodeId != config.BrokerId)
            .ToArray();
    }

    /// <summary>Whether a quorum was configured at all — false is combined mode.</summary>
    public bool HasConfiguredQuorum => _quorum.Count > 0;

    /// <summary>
    /// Whether every configured voter has been tried without one ever answering.
    /// </summary>
    /// <remarks>
    /// The bootstrap corner: on a first start there is no log to learn from, so the configured
    /// list is the whole truth and a mistyped entry looks exactly like a network problem. This
    /// is what lets the caller say which it is.
    /// </remarks>
    public bool ExhaustedQuorumWithoutContact =>
        HasConfiguredQuorum && !_everSucceeded && _consecutiveFailures >= _quorum.Count;

    /// <summary>The configured endpoints, for an error message that can name them.</summary>
    public string DescribeConfiguredQuorum() =>
        string.Join(", ", _quorum.Select(v => $"{v.NodeId}@{v.Host}:{v.Port}"));

    /// <summary>
    /// The endpoint to try next, or <c>null</c> when this broker knows of nobody to ask —
    /// which is the seed's own case, and standalone.
    /// </summary>
    public (string Host, int ReplicationPort)? Resolve()
    {
        var controllerId = _clusterState.ControllerId;

        // We ARE the controller — there is nobody to register with. No fallback, or the
        // controller would try to register against a follower and loop on NotController.
        if (controllerId == _config.BrokerId)
            return null;

        if (_consecutiveFailures < FailuresBeforeRotating
            && controllerId >= 0
            && _clusterState.GetBroker(controllerId) is { } controller)
        {
            return (controller.Host, controller.ReplicationPort);
        }

        // The configured quorum, one voter per attempt. Ordered by the rotation counter rather
        // than always starting at the first entry, so a voter that is down costs one attempt
        // instead of blocking every one.
        if (_quorum.Count > 0)
        {
            // Having decided the named controller is not answering, do not hand it back
            // because the rotation happened to land on it — that is the wait this branch
            // exists to end.
            var abandoned = _consecutiveFailures >= FailuresBeforeRotating ? controllerId : -1;

            for (var offset = 0; offset < _quorum.Count; offset++)
            {
                var candidate = _quorum[(_rotation + offset) % _quorum.Count];
                if (candidate.NodeId != abandoned)
                    return EndpointFor(candidate);
            }

            // A one-voter quorum, and that voter is the one being abandoned. There is nobody
            // else to ask, so ask it again rather than answering "no controller" — it may
            // have come back, and a resolver that gives up here never recovers.
            return EndpointFor(_quorum[_rotation % _quorum.Count]);
        }

        // No quorum configured: combined mode, where the lowest-id peer is the controller by
        // convention and every broker is a candidate.
        BrokerNode? seed = null;
        foreach (var kvp in _clusterState.Brokers)
        {
            var broker = kvp.Value;
            if (broker.BrokerId == _config.BrokerId)
                continue;
            if (seed is null || broker.BrokerId < seed.BrokerId)
                seed = broker;
        }

        return seed is null ? null : (seed.Host, seed.ReplicationPort);
    }

    /// <summary>
    /// Where to reach a voter: what the cluster reports about itself when it knows, else what
    /// was configured.
    /// </summary>
    /// <remarks>
    /// The configured list only has to be good enough for first contact. Once the cluster has
    /// said where a node actually is, that is newer than what an operator typed — which is what
    /// makes a list that has gone stale survivable rather than fatal.
    /// </remarks>
    private (string Host, int ReplicationPort) EndpointFor(ControllerQuorumVoter voter)
        => _clusterState.GetBroker(voter.NodeId) is { } known
            ? (known.Host, known.ReplicationPort)
            : (voter.Host, voter.Port);

    /// <summary>Records that the last resolved endpoint answered.</summary>
    public void ReportSuccess()
    {
        _consecutiveFailures = 0;
        _everSucceeded = true;
    }

    /// <summary>Records that the last resolved endpoint did not answer, and moves on.</summary>
    public void ReportFailure()
    {
        _consecutiveFailures++;
        _rotation++;
    }
}

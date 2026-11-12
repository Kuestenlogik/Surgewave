namespace Kuestenlogik.Surgewave.Broker.Hosting;

/// <summary>
/// Whether this broker runs the embedded Connect worker as one broker of several.
/// </summary>
/// <remarks>
/// The embedded worker reads and writes its stores — the connector configs and the pipeline definitions — through its
/// own broker, and the workers of a cluster's brokers do not form a group (#282). Only the worker on the broker that
/// leads the stores' partitions has the stored connectors and pipelines; the others start without them and answer a
/// change with an error. That is to be something the operator has seen, so the broker says it at every start.
/// </remarks>
public static class EmbeddedConnectInCluster
{
    /// <summary>
    /// True when <paramref name="config"/> enables the embedded Connect worker and names more than one broker, as a
    /// cluster node or as a controller voter.
    /// </summary>
    public static bool Applies(BrokerConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        return config.Connect.Enabled
            && (Count(config.ClusterNodes) > 1 || Count(config.ControllerQuorumVoters) > 1);
    }

    private static int Count(string? list) =>
        string.IsNullOrWhiteSpace(list)
            ? 0
            : list.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Length;
}

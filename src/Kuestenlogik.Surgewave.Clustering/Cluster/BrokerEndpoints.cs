namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// What a broker's registration says about where to reach it (#171).
/// </summary>
/// <remarks>
/// The shape a metadata-log registration entry needs, resolved once from the listener set so the
/// proposer and the apply cannot read that set differently.
/// </remarks>
/// <param name="Host">Client-facing host.</param>
/// <param name="Port">Client-facing port.</param>
/// <param name="InterBrokerProtocol">Highest inter-broker protocol level this broker advertises.</param>
/// <param name="ReplicationPort">
/// Inter-broker port, or <c>null</c> when this registration advertised no REPLICATION listener — in
/// which case a previously discovered one is kept rather than overwritten.
/// </param>
public readonly record struct BrokerEndpoints(string Host, int Port, short InterBrokerProtocol, int? ReplicationPort);

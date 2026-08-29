namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Commits a broker's registration to the metadata log (#171).
/// </summary>
/// <remarks>
/// <para>
/// A joining broker used to be registered straight into the controller's membership store, which
/// minted a composed epoch locally. Only the controller's own registration reached the log, so the
/// rule the rest of the system was built on — a broker epoch IS the committed index of its
/// registration entry — held for exactly one broker in the cluster.
/// </para>
/// <para>
/// That is why "is this broker caught up" had no answer for anyone else: the comparison is against
/// the index of the broker's own registration, and there was none to compare with. Going through
/// the log is what makes the question answerable, and Kafka's unfence rule with it.
/// </para>
/// <para>
/// Separate from <see cref="IIsrUpdateApplier"/> because it answers a different question — the
/// inter-broker service needs to know who may commit a registration, not who may apply an ISR.
/// </para>
/// </remarks>
public interface IBrokerRegistrar
{
    /// <summary>Whether this broker is currently the controller, and may therefore propose.</summary>
    bool IsController { get; }

    /// <summary>
    /// Commits a registration entry and returns once it has been applied, or <c>false</c> when this
    /// broker is not the leader or the entry did not commit in time.
    /// </summary>
    /// <remarks>
    /// The epoch is not returned here: the applied entry's index is the epoch only for a NEW
    /// incarnation. A broker re-registering with the same incarnation keeps the epoch it already
    /// has, and the membership store is the one that knows which case this was — so the caller
    /// reads the epoch back from there rather than assuming the index.
    /// </remarks>
    Task<bool> RegisterBrokerViaRaftAsync(
        int brokerId,
        string host,
        int port,
        string? rack,
        Guid incarnationId,
        short interBrokerProtocol,
        int replicationPort,
        CancellationToken ct);
}

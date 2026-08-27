namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// What happened when a broker was asked to be removed from the cluster (#129).
/// </summary>
/// <remarks>
/// A <c>bool</c> said only "not removed", which is three different situations: this
/// node is not the controller and cannot decide, the id does not name a broker, or
/// the entry was proposed and did not commit in time. An operator needs to tell
/// those apart — the first is "ask the controller", the second is "check what you
/// typed", the third is "the cluster is in trouble". Distinguishing them costs an
/// enum, and the method had no callers yet, so nothing had to be migrated.
/// </remarks>
public enum BrokerRemovalOutcome
{
    /// <summary>The removal was proposed and committed.</summary>
    Removed,

    /// <summary>
    /// This node is not the controller, so it cannot propose. Not an error: the
    /// caller should re-issue against the current controller.
    /// </summary>
    NotController,

    /// <summary>
    /// No broker with that id is known. Refused rather than committed: an entry
    /// removing a broker that was never there does nothing, and replicating it
    /// would make a typo look like a successful decommission.
    /// </summary>
    UnknownBroker,

    /// <summary>
    /// The controller was asked to remove itself while it is the active one. Kafka
    /// refuses the same thing ("Controller cannot unregister itself while it is
    /// active", KIP-1312) and for the same reason: the node proposing the entry is
    /// the node that has to replicate and apply it, so it would be dismantling the
    /// identity it is using to do the work.
    /// </summary>
    CannotRemoveSelf,

    /// <summary>
    /// The entry was proposed but did not commit within the timeout. The cluster may
    /// still commit it afterwards, so a retry has to tolerate the removal having
    /// happened — which it does, the apply being idempotent.
    /// </summary>
    NotCommitted
}

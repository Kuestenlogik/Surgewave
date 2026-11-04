namespace Kuestenlogik.Surgewave.Coordination.Coordinators;

/// <summary>What a coordinator coordinates — Kafka's FindCoordinator key types.</summary>
public enum CoordinatorKind : byte
{
    /// <summary>A consumer, share or streams group, keyed by group id.</summary>
    Group = 0,

    /// <summary>A transactional producer, keyed by transactional id.</summary>
    Transaction = 1,

    /// <summary>A share partition's state, keyed by <c>group:topicId:partition</c> (KIP-932, #228).</summary>
    Share = 2,
}

/// <summary>Whether a coordinator could be named.</summary>
public enum CoordinatorLookupStatus
{
    /// <summary>The coordinator is named.</summary>
    Ok,

    /// <summary>
    /// No broker coordinates the key right now: the coordinator topic does not exist yet, or its partition has no
    /// leader. A client retries — Kafka's COORDINATOR_NOT_AVAILABLE.
    /// </summary>
    NotAvailable,
}

/// <summary>The broker that coordinates a key, or why none does.</summary>
public readonly record struct CoordinatorLocation(CoordinatorLookupStatus Status, int BrokerId, string Host, int Port)
{
    /// <summary>No coordinator right now.</summary>
    public static CoordinatorLocation NotAvailable { get; } = new(CoordinatorLookupStatus.NotAvailable, -1, string.Empty, -1);
}

/// <summary>
/// Which broker coordinates a group or a transactional id (#204). In a cluster, the leader of the key's partition of
/// the coordinator topic (Kafka's <c>__consumer_offsets</c> / <c>__transaction_state</c>); a broker on its own
/// coordinates everything. One answer for every interface — Kafka, native, gRPC — so a client cannot be sent to two
/// coordinators for one group.
/// </summary>
public interface ICoordinatorLocator
{
    /// <summary>The broker that coordinates <paramref name="key"/>.</summary>
    CoordinatorLocation Locate(CoordinatorKind kind, string key);

    /// <summary>Whether this broker coordinates <paramref name="key"/> — the NOT_COORDINATOR check.</summary>
    bool IsLocal(CoordinatorKind kind, string key);
}

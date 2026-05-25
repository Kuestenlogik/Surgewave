namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Types of metadata commands that can be stored in the Raft log.
/// </summary>
public enum MetadataCommandType
{
    /// <summary>
    /// No operation - used for leader confirmation.
    /// </summary>
    Noop = 0,

    /// <summary>
    /// Register a new broker in the cluster.
    /// </summary>
    BrokerRegistered = 1,

    /// <summary>
    /// Broker has been removed from the cluster.
    /// </summary>
    BrokerRemoved = 2,

    /// <summary>
    /// Create a new topic.
    /// </summary>
    TopicCreated = 3,

    /// <summary>
    /// Delete a topic.
    /// </summary>
    TopicDeleted = 4,

    /// <summary>
    /// Assign partitions to brokers.
    /// </summary>
    PartitionAssigned = 5,

    /// <summary>
    /// Change the ISR for a partition.
    /// </summary>
    IsrChanged = 6,

    /// <summary>
    /// Change the leader for a partition.
    /// </summary>
    LeaderChanged = 7,

    /// <summary>
    /// Update topic configuration.
    /// </summary>
    ConfigChanged = 8,

    /// <summary>
    /// KIP-853 voter-set change: the payload carries the new
    /// <see cref="RaftConfiguration"/> resulting from an Add/Remove/Update
    /// voter operation. Reserved here so a future online-reconfiguration
    /// implementation can replay history through the existing Raft log
    /// machinery without a schema migration. Surgewave does not yet propose
    /// these entries — operators reconfigure the static voter set and
    /// restart; see <c>docs/raft/voter-changes.md</c>.
    /// </summary>
    VoterChange = 9
}

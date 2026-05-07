namespace Kuestenlogik.Surgewave.Broker.Audit;

/// <summary>
/// Types of events that can be audited.
/// </summary>
public enum AuditEventType
{
    /// <summary>
    /// A topic was created.
    /// </summary>
    TopicCreated,

    /// <summary>
    /// A topic was deleted.
    /// </summary>
    TopicDeleted,

    /// <summary>
    /// Topic configuration was altered.
    /// </summary>
    TopicAltered,

    /// <summary>
    /// Partitions were added to a topic.
    /// </summary>
    PartitionsAdded,

    /// <summary>
    /// Records were deleted from a topic.
    /// </summary>
    RecordsDeleted,

    /// <summary>
    /// An ACL was created.
    /// </summary>
    AclCreated,

    /// <summary>
    /// An ACL was deleted.
    /// </summary>
    AclDeleted,

    /// <summary>
    /// A connector was created.
    /// </summary>
    ConnectorCreated,

    /// <summary>
    /// A connector was deleted.
    /// </summary>
    ConnectorDeleted,

    /// <summary>
    /// A connector was paused.
    /// </summary>
    ConnectorPaused,

    /// <summary>
    /// A connector was resumed.
    /// </summary>
    ConnectorResumed,

    /// <summary>
    /// Broker configuration was changed.
    /// </summary>
    ConfigChanged,

    /// <summary>
    /// A user authentication was attempted.
    /// </summary>
    AuthenticationAttempt,

    /// <summary>
    /// A user authentication succeeded.
    /// </summary>
    AuthenticationSuccess,

    /// <summary>
    /// A user authentication failed.
    /// </summary>
    AuthenticationFailed,

    /// <summary>
    /// Authorization check was performed.
    /// </summary>
    AuthorizationCheck,

    /// <summary>
    /// A consumer group was deleted.
    /// </summary>
    ConsumerGroupDeleted,

    /// <summary>
    /// Offsets were committed for a consumer group.
    /// </summary>
    OffsetsCommitted,

    /// <summary>
    /// A transaction was committed.
    /// </summary>
    TransactionCommitted,

    /// <summary>
    /// A transaction was aborted.
    /// </summary>
    TransactionAborted,

    /// <summary>
    /// A schema was registered.
    /// </summary>
    SchemaRegistered,

    /// <summary>
    /// A schema was deleted.
    /// </summary>
    SchemaDeleted
}

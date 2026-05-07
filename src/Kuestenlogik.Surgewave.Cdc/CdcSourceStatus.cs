namespace Kuestenlogik.Surgewave.Cdc;

/// <summary>
/// Represents the runtime status of a CDC source.
/// </summary>
public sealed record CdcSourceStatus
{
    /// <summary>
    /// Unique identifier for this CDC source instance.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// The database type (e.g., "PostgreSQL").
    /// </summary>
    public required string DatabaseType { get; init; }

    /// <summary>
    /// Current state of the CDC source.
    /// </summary>
    public required CdcSourceState State { get; init; }

    /// <summary>
    /// The replication slot name being used.
    /// </summary>
    public required string SlotName { get; init; }

    /// <summary>
    /// The publication name being used.
    /// </summary>
    public required string PublicationName { get; init; }

    /// <summary>
    /// Number of tables being tracked.
    /// </summary>
    public int TrackedTables { get; init; }

    /// <summary>
    /// Total number of events captured since startup.
    /// </summary>
    public long EventsCaptured { get; init; }

    /// <summary>
    /// Last confirmed LSN position.
    /// </summary>
    public long LastLsn { get; init; }

    /// <summary>
    /// Timestamp of the last captured event.
    /// </summary>
    public DateTimeOffset? LastEventTimestamp { get; init; }

    /// <summary>
    /// Error message if the source is in a faulted state.
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// When this CDC source was started.
    /// </summary>
    public DateTimeOffset StartedAt { get; init; }
}

/// <summary>
/// Possible states for a CDC source.
/// </summary>
public enum CdcSourceState
{
    /// <summary>
    /// Source is being initialized (creating slot, publication, etc.).
    /// </summary>
    Initializing,

    /// <summary>
    /// Source is performing an initial snapshot of existing data.
    /// </summary>
    Snapshotting,

    /// <summary>
    /// Source is actively streaming changes from the WAL.
    /// </summary>
    Streaming,

    /// <summary>
    /// Source has been stopped.
    /// </summary>
    Stopped,

    /// <summary>
    /// Source encountered an error and stopped.
    /// </summary>
    Faulted
}

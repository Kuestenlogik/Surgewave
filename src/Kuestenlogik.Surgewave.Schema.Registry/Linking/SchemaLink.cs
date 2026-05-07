namespace Kuestenlogik.Surgewave.Schema.Registry.Linking;

/// <summary>
/// Tracks the synchronization state of a schema between two clusters.
/// </summary>
public sealed class SchemaLink
{
    /// <summary>
    /// The subject name being linked.
    /// </summary>
    public required string Subject { get; init; }

    /// <summary>
    /// The cluster that owns the source version.
    /// </summary>
    public required string SourceCluster { get; init; }

    /// <summary>
    /// The cluster that received the synced version.
    /// </summary>
    public required string TargetCluster { get; init; }

    /// <summary>
    /// Schema version on the source cluster.
    /// </summary>
    public int SourceVersion { get; init; }

    /// <summary>
    /// Schema version on the target cluster.
    /// </summary>
    public int TargetVersion { get; init; }

    /// <summary>
    /// Current synchronization status.
    /// </summary>
    public SchemaSyncStatus Status { get; set; } = SchemaSyncStatus.Synced;

    /// <summary>
    /// Timestamp of the last successful synchronization.
    /// </summary>
    public DateTimeOffset LastSyncedAt { get; set; }

    /// <summary>
    /// Optional error message when status is Failed.
    /// </summary>
    public string? ErrorMessage { get; set; }
}

/// <summary>
/// Status of a schema link synchronization.
/// </summary>
public enum SchemaSyncStatus
{
    /// <summary>
    /// Schema is fully synchronized between source and target.
    /// </summary>
    Synced,

    /// <summary>
    /// Schema synchronization is pending (new version detected, not yet synced).
    /// </summary>
    Pending,

    /// <summary>
    /// A conflict was detected between source and target schemas.
    /// </summary>
    Conflict,

    /// <summary>
    /// Schema synchronization failed with an error.
    /// </summary>
    Failed
}

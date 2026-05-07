namespace Kuestenlogik.Surgewave.Schema.Registry.Linking;

/// <summary>
/// Tracks metrics for schema linking synchronization operations.
/// </summary>
public sealed class SchemaLinkingMetrics
{
    /// <summary>
    /// Total number of schemas successfully synced.
    /// </summary>
    public long SchemasSynced { get; set; }

    /// <summary>
    /// Total number of conflicts detected.
    /// </summary>
    public long ConflictsDetected { get; set; }

    /// <summary>
    /// Total number of conflicts automatically resolved.
    /// </summary>
    public long ConflictsResolved { get; set; }

    /// <summary>
    /// Total number of sync errors encountered.
    /// </summary>
    public long SyncErrors { get; set; }

    /// <summary>
    /// Timestamp of the last sync cycle completion.
    /// </summary>
    public DateTimeOffset? LastSyncAt { get; set; }

    /// <summary>
    /// Number of schemas synced per remote cluster.
    /// </summary>
    public Dictionary<string, int> PerClusterSyncCount { get; set; } = [];

    /// <summary>
    /// Records a successful schema sync for a cluster.
    /// </summary>
    public void RecordSync(string clusterId)
    {
        SchemasSynced++;
        PerClusterSyncCount.TryGetValue(clusterId, out var count);
        PerClusterSyncCount[clusterId] = count + 1;
    }

    /// <summary>
    /// Records a detected conflict.
    /// </summary>
    public void RecordConflict()
    {
        ConflictsDetected++;
    }

    /// <summary>
    /// Records a resolved conflict.
    /// </summary>
    public void RecordConflictResolved()
    {
        ConflictsResolved++;
    }

    /// <summary>
    /// Records a sync error.
    /// </summary>
    public void RecordError()
    {
        SyncErrors++;
    }

    /// <summary>
    /// Marks the last sync cycle as complete.
    /// </summary>
    public void RecordSyncCycleComplete()
    {
        LastSyncAt = DateTimeOffset.UtcNow;
    }
}

using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;

/// <summary>
/// A materialized view: its <see cref="ViewDefinition"/> plus the current
/// in-memory result snapshot maintained by the refresh loop.
///
/// State is replaced atomically on each refresh; readers always see a
/// consistent (but possibly slightly stale) snapshot.
/// </summary>
public sealed class MaterializedView
{
    private volatile MaterializedViewSnapshot _snapshot;
    private readonly ConcurrentDictionary<int, long> _partitionOffsets = new();

    public MaterializedView(ViewDefinition definition)
    {
        Definition = definition;
        _snapshot = new MaterializedViewSnapshot(
            Rows: [],
            ColumnNames: [],
            UpdatedAt: DateTimeOffset.UtcNow,
            RefreshCount: 0);
    }

    /// <summary>Static metadata of the view.</summary>
    public ViewDefinition Definition { get; }

    /// <summary>The most recently published snapshot. Volatile read.</summary>
    public MaterializedViewSnapshot Snapshot => _snapshot;

    /// <summary>
    /// Replaces the snapshot atomically. Called by the refresh loop after
    /// re-executing the underlying SELECT against the latest source rows.
    /// </summary>
    public void PublishSnapshot(
        List<Dictionary<string, object?>> rows,
        List<string> columnNames)
    {
        _snapshot = new MaterializedViewSnapshot(
            Rows: rows,
            ColumnNames: columnNames,
            UpdatedAt: DateTimeOffset.UtcNow,
            RefreshCount: _snapshot.RefreshCount + 1);
    }

    /// <summary>
    /// Records the highest offset processed for a partition. Used by the
    /// refresh loop to resume tailing without re-reading already-seen data
    /// (when full re-aggregation is not required).
    /// </summary>
    public void RecordPartitionOffset(int partition, long offset)
        => _partitionOffsets[partition] = offset;

    /// <summary>Returns the last processed offset for a partition, or -1 when never seen.</summary>
    public long GetPartitionOffset(int partition)
        => _partitionOffsets.TryGetValue(partition, out var off) ? off : -1L;

    /// <summary>Returns a snapshot of all (partition, offset) checkpoints.</summary>
    public IReadOnlyDictionary<int, long> GetCheckpoints()
        => new Dictionary<int, long>(_partitionOffsets);
}

/// <summary>
/// Immutable snapshot of a materialized view's current contents.
/// </summary>
public sealed record MaterializedViewSnapshot(
    List<Dictionary<string, object?>> Rows,
    List<string> ColumnNames,
    DateTimeOffset UpdatedAt,
    long RefreshCount);

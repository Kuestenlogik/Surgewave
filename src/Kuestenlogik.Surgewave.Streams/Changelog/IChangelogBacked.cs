namespace Kuestenlogik.Surgewave.Streams.Changelog;

/// <summary>
/// Marker interface for state stores that are backed by a changelog topic.
/// Provides metadata needed for changelog restoration without requiring
/// knowledge of the generic type parameters at runtime.
/// </summary>
public interface IChangelogBacked
{
    /// <summary>
    /// The changelog topic name for this store.
    /// </summary>
    string ChangelogTopicName { get; }

    /// <summary>
    /// The partition to restore from.
    /// </summary>
    int ChangelogPartition { get; }

    /// <summary>
    /// Restores a single record from the changelog.
    /// Tombstones (empty value) trigger a delete.
    /// </summary>
    void RestoreRecord(byte[] key, byte[] value);
}

namespace Kuestenlogik.Surgewave.Streams.Changelog;

/// <summary>
/// Represents a point-in-time snapshot reference for a state store.
/// Contains the changelog offset and metadata needed for incremental recovery.
/// </summary>
public sealed class StoreCheckpoint
{
    /// <summary>
    /// The name of the state store this checkpoint belongs to.
    /// </summary>
    public required string StoreName { get; init; }

    /// <summary>
    /// The changelog offset up to which this checkpoint covers.
    /// On recovery, only records after this offset need to be replayed.
    /// </summary>
    public required long ChangelogOffset { get; init; }

    /// <summary>
    /// When this checkpoint was created (Unix milliseconds).
    /// </summary>
    public required long TimestampMs { get; init; }

    /// <summary>
    /// Serialized snapshot data for in-memory stores.
    /// For persistent stores, this may be null if the snapshot is on disk.
    /// </summary>
    public byte[]? SnapshotData { get; init; }

    /// <summary>
    /// Number of entries in the store at checkpoint time.
    /// </summary>
    public long EntryCount { get; init; }
}

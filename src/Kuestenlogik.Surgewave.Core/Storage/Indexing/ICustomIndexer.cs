namespace Kuestenlogik.Surgewave.Core.Storage.Indexing;

/// <summary>
/// Interface for custom indexers that can build and query secondary indexes
/// based on record content (headers, keys, values).
/// </summary>
public interface ICustomIndexer : IDisposable
{
    /// <summary>
    /// Unique name for this indexer (used for file naming, e.g., "vectorclock" -> ".vectorclockindex")
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Called after a batch is successfully appended to the log.
    /// Implementations should parse the batch and update their index.
    /// </summary>
    /// <param name="baseOffset">The base offset of the appended batch</param>
    /// <param name="filePosition">The file position where the batch was written</param>
    /// <param name="recordBatch">The raw Kafka RecordBatch bytes</param>
    void OnBatchAppended(long baseOffset, long filePosition, ReadOnlySpan<byte> recordBatch);

    /// <summary>
    /// Flush pending index entries to persistent storage.
    /// </summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Load the index from persistent storage.
    /// </summary>
    /// <param name="indexDirectory">Directory containing index files</param>
    /// <param name="segmentBaseOffset">Base offset of the segment this index belongs to</param>
    void Load(string indexDirectory, long segmentBaseOffset);

    /// <summary>
    /// Save the index to persistent storage.
    /// </summary>
    /// <param name="indexDirectory">Directory to save index files</param>
    /// <param name="segmentBaseOffset">Base offset of the segment this index belongs to</param>
    ValueTask SaveAsync(string indexDirectory, long segmentBaseOffset, CancellationToken cancellationToken = default);

    /// <summary>
    /// Delete index files associated with this indexer.
    /// </summary>
    /// <param name="indexDirectory">Directory containing index files</param>
    /// <param name="segmentBaseOffset">Base offset of the segment</param>
    void DeleteFiles(string indexDirectory, long segmentBaseOffset);
}

/// <summary>
/// Result of a custom index lookup operation.
/// </summary>
/// <param name="Offset">The message offset that matched the query</param>
/// <param name="FilePosition">The file position of the batch containing the message</param>
public readonly record struct IndexLookupResult(long Offset, long FilePosition);

/// <summary>
/// Interface for custom indexers that support key-based lookups.
/// </summary>
public interface IKeyBasedIndexer : ICustomIndexer
{
    /// <summary>
    /// Lookup entries by a binary key.
    /// </summary>
    /// <param name="key">The index key to search for</param>
    /// <returns>Matching index entries, or empty if not found</returns>
    IReadOnlyList<IndexLookupResult> Lookup(ReadOnlySpan<byte> key);

    /// <summary>
    /// Lookup entries by a string key.
    /// </summary>
    /// <param name="key">The index key to search for</param>
    /// <returns>Matching index entries, or empty if not found</returns>
    IReadOnlyList<IndexLookupResult> Lookup(string key);
}

/// <summary>
/// Interface for custom indexers that support range queries (e.g., vector clocks, sequences).
/// </summary>
public interface IRangeIndexer : ICustomIndexer
{
    /// <summary>
    /// Find entries where the indexed value is >= minValue.
    /// </summary>
    /// <param name="indexKey">The index key (e.g., node ID for vector clocks)</param>
    /// <param name="minValue">Minimum value (inclusive)</param>
    /// <returns>Matching index entries in ascending order by value</returns>
    IReadOnlyList<IndexLookupResult> LookupGreaterOrEqual(ReadOnlySpan<byte> indexKey, long minValue);

    /// <summary>
    /// Find entries within a value range.
    /// </summary>
    /// <param name="indexKey">The index key (e.g., node ID for vector clocks)</param>
    /// <param name="minValue">Minimum value (inclusive)</param>
    /// <param name="maxValue">Maximum value (inclusive)</param>
    /// <returns>Matching index entries in ascending order by value</returns>
    IReadOnlyList<IndexLookupResult> LookupRange(ReadOnlySpan<byte> indexKey, long minValue, long maxValue);
}

/// <summary>
/// Factory for creating custom indexers.
/// </summary>
public interface ICustomIndexerFactory
{
    /// <summary>
    /// Create a new instance of the custom indexer.
    /// </summary>
    ICustomIndexer Create();
}

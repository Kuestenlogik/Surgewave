namespace Kuestenlogik.Surgewave.Storage.Tiering;

/// <summary>
/// Represents a remote segment with its metadata
/// </summary>
public sealed record RemoteSegmentInfo(
    string Topic,
    int Partition,
    long BaseOffset,
    long Size,
    DateTimeOffset CreatedAt,
    DateTimeOffset UploadedAt);

/// <summary>
/// Data container for all segment files to upload.
/// Mirrors Kafka's LogSegmentData.
/// </summary>
public sealed class LogSegmentData
{
    /// <summary>Path to the log file</summary>
    public required string LogPath { get; init; }

    /// <summary>Path to the offset index file</summary>
    public required string OffsetIndexPath { get; init; }

    /// <summary>Path to the time index file</summary>
    public required string TimeIndexPath { get; init; }

    /// <summary>Path to the transaction index file (optional)</summary>
    public string? TransactionIndexPath { get; init; }

    /// <summary>Path to the producer snapshot file (optional)</summary>
    public string? ProducerSnapshotPath { get; init; }

    /// <summary>Leader epoch index data</summary>
    public byte[]? LeaderEpochIndex { get; init; }
}

/// <summary>
/// Custom metadata returned by the storage provider (max 128 bytes).
/// </summary>
public sealed class CustomMetadata
{
    public const int MaxSize = 128;

    public byte[] Data { get; }

    public CustomMetadata(byte[] data)
    {
        if (data.Length > MaxSize)
            throw new ArgumentException($"Custom metadata exceeds max size of {MaxSize} bytes", nameof(data));
        Data = data;
    }
}

/// <summary>
/// Abstraction for remote storage backends (Azure Blob, S3, local filesystem, etc.)
/// Used for tiered storage to offload old log segments.
/// Mirrors Kafka's RemoteStorageManager interface (KIP-405).
/// </summary>
public interface IRemoteStorageProvider : IAsyncDisposable
{
    /// <summary>
    /// Upload a complete segment (log + index + timeindex) to remote storage
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="baseOffset">Base offset of the segment</param>
    /// <param name="logData">Raw log file bytes</param>
    /// <param name="indexData">Offset index bytes</param>
    /// <param name="timeIndexData">Time index bytes</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task UploadSegmentAsync(
        string topic,
        int partition,
        long baseOffset,
        ReadOnlyMemory<byte> logData,
        ReadOnlyMemory<byte> indexData,
        ReadOnlyMemory<byte> timeIndexData,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Upload a complete segment with all indexes (Kafka-compatible).
    /// Returns optional custom metadata from the storage provider.
    /// </summary>
    /// <param name="segmentId">Unique segment identifier</param>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="baseOffset">Base offset of the segment</param>
    /// <param name="segmentData">All segment files to upload</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Optional custom metadata from the provider</returns>
    Task<CustomMetadata?> CopyLogSegmentDataAsync(
        Guid segmentId,
        string topic,
        int partition,
        long baseOffset,
        LogSegmentData segmentData,
        CancellationToken cancellationToken = default)
    {
        // Default implementation for backward compatibility
        return Task.FromResult<CustomMetadata?>(null);
    }

    /// <summary>
    /// Download a segment from remote storage
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="baseOffset">Base offset of the segment</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Tuple of (logData, indexData, timeIndexData)</returns>
    Task<(byte[] LogData, byte[] IndexData, byte[] TimeIndexData)> DownloadSegmentAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Fetch log segment data as a stream (range-based).
    /// Mirrors Kafka's RemoteStorageManager.fetchLogSegment().
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="baseOffset">Base offset of the segment</param>
    /// <param name="startPosition">Start position within the segment</param>
    /// <param name="endPosition">Optional end position (exclusive)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Stream of segment data</returns>
    Task<Stream> FetchLogSegmentAsync(
        string topic,
        int partition,
        long baseOffset,
        int startPosition,
        int? endPosition = null,
        CancellationToken cancellationToken = default)
    {
        // Default implementation: download entire segment and seek
        return Task.FromResult<Stream>(Stream.Null);
    }

    /// <summary>
    /// Fetch an index file from remote storage.
    /// Mirrors Kafka's RemoteStorageManager.fetchIndex().
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="baseOffset">Base offset of the segment</param>
    /// <param name="indexType">Type of index to fetch</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Stream of index data</returns>
    Task<Stream> FetchIndexAsync(
        string topic,
        int partition,
        long baseOffset,
        RemoteIndexType indexType,
        CancellationToken cancellationToken = default)
    {
        // Default implementation: return empty stream
        return Task.FromResult<Stream>(Stream.Null);
    }

    /// <summary>
    /// Delete a segment from remote storage.
    /// This operation is idempotent - missing resources should not throw.
    /// </summary>
    Task DeleteSegmentAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// List all segments for a topic-partition in remote storage
    /// </summary>
    Task<IReadOnlyList<RemoteSegmentInfo>> ListSegmentsAsync(
        string topic,
        int partition,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Check if a segment exists in remote storage
    /// </summary>
    Task<bool> SegmentExistsAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get metadata for a specific segment
    /// </summary>
    Task<RemoteSegmentInfo?> GetSegmentInfoAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default);
}

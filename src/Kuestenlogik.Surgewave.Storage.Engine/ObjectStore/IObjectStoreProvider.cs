namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Minimal object storage provider interface for the ObjectStoreEngine.
/// Decoupled from IRemoteStorageProvider to avoid circular project references.
/// Implementations can bridge to IRemoteStorageProvider, S3, Azure Blob, GCP, or local filesystem.
/// </summary>
public interface IObjectStoreProvider
{
    /// <summary>
    /// Upload segment data to object storage.
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="baseOffset">Base offset of the segment</param>
    /// <param name="data">Raw segment data</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task UploadAsync(
        string topic,
        int partition,
        long baseOffset,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Download segment data from object storage.
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="baseOffset">Base offset of the segment</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Segment data, or null if not found.</returns>
    Task<byte[]?> DownloadAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Delete segment data from object storage.
    /// </summary>
    Task DeleteAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// List all segment base offsets for a topic-partition.
    /// </summary>
    Task<IReadOnlyList<long>> ListSegmentOffsetsAsync(
        string topic,
        int partition,
        CancellationToken cancellationToken = default);
}

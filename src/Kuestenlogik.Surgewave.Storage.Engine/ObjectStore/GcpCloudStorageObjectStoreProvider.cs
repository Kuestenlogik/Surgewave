using Google.Cloud.Storage.V1;

namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Google Cloud Storage implementation of IObjectStoreProvider.
/// Stores segments in GCS with topic/partition/offset-based object paths.
/// </summary>
public sealed class GcpCloudStorageObjectStoreProvider : IObjectStoreProvider, IDisposable
{
    private readonly StorageClient _storageClient;
    private readonly string _bucketName;
    private readonly string? _prefix;
    private readonly bool _ownsClient;

    /// <summary>
    /// Create a new GCP Cloud Storage object store provider.
    /// </summary>
    /// <param name="bucketName">GCS bucket name</param>
    /// <param name="prefix">Optional object path prefix (e.g., "surgewave/data")</param>
    /// <param name="client">Optional pre-configured StorageClient. If null, creates one from default credentials.</param>
    public GcpCloudStorageObjectStoreProvider(string bucketName, string? prefix = null, StorageClient? client = null)
    {
        _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        _prefix = prefix?.TrimEnd('/');

        if (client != null)
        {
            _storageClient = client;
            _ownsClient = false;
        }
        else
        {
            _storageClient = StorageClient.Create();
            _ownsClient = true;
        }
    }

    /// <inheritdoc />
    public async Task UploadAsync(
        string topic,
        int partition,
        long baseOffset,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        var objectName = ObjectStoreKeyFormatter.FormatKey(_prefix, topic, partition, baseOffset);

        using var stream = new MemoryStream(data.ToArray());
        await _storageClient.UploadObjectAsync(
            _bucketName,
            objectName,
            "application/octet-stream",
            stream,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<byte[]?> DownloadAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var objectName = ObjectStoreKeyFormatter.FormatKey(_prefix, topic, partition, baseOffset);

        try
        {
            using var memoryStream = new MemoryStream();
            await _storageClient.DownloadObjectAsync(
                _bucketName,
                objectName,
                memoryStream,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return memoryStream.ToArray();
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public async Task DeleteAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var objectName = ObjectStoreKeyFormatter.FormatKey(_prefix, topic, partition, baseOffset);

        try
        {
            await _storageClient.DeleteObjectAsync(
                _bucketName,
                objectName,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // Object doesn't exist, which is fine for idempotent delete.
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<long>> ListSegmentOffsetsAsync(
        string topic,
        int partition,
        CancellationToken cancellationToken = default)
    {
        var listPrefix = ObjectStoreKeyFormatter.FormatListPrefix(_prefix, topic, partition);
        var offsets = new List<long>();

        // StorageClient.ListObjects is synchronous but returns a pageable.
        // We use the sync API and wrap it since ListObjectsAsync returns a PagedAsyncEnumerable
        // which requires different handling.
        var objects = _storageClient.ListObjects(_bucketName, listPrefix);
        foreach (var obj in objects)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var offset = ObjectStoreKeyFormatter.ParseOffsetFromKey(obj.Name);
            if (offset.HasValue)
            {
                offsets.Add(offset.Value);
            }
        }

        offsets.Sort();
        return Task.FromResult<IReadOnlyList<long>>(offsets);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_ownsClient)
        {
            _storageClient.Dispose();
        }
    }
}

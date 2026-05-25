using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Azure Blob Storage implementation of IObjectStoreProvider.
/// Stores segments in Azure Blob Storage with topic/partition/offset-based blob paths.
/// </summary>
public sealed class AzureBlobObjectStoreProvider : IObjectStoreProvider, IDisposable
{
    private readonly BlobContainerClient _containerClient;
    private readonly string? _prefix;

    /// <summary>
    /// Create a new Azure Blob Storage object store provider from a connection string.
    /// </summary>
    /// <param name="connectionString">Azure Storage connection string</param>
    /// <param name="containerName">Blob container name</param>
    /// <param name="prefix">Optional blob path prefix (e.g., "surgewave/data")</param>
    public AzureBlobObjectStoreProvider(string connectionString, string containerName, string? prefix = null)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        ArgumentNullException.ThrowIfNull(containerName);

        var blobServiceClient = new BlobServiceClient(connectionString);
        _containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        _prefix = prefix?.TrimEnd('/');
    }

    /// <summary>
    /// Create a provider from an existing BlobServiceClient.
    /// </summary>
    /// <param name="blobServiceClient">Pre-configured BlobServiceClient</param>
    /// <param name="containerName">Blob container name</param>
    /// <param name="prefix">Optional blob path prefix</param>
    public AzureBlobObjectStoreProvider(BlobServiceClient blobServiceClient, string containerName, string? prefix = null)
    {
        ArgumentNullException.ThrowIfNull(blobServiceClient);
        ArgumentNullException.ThrowIfNull(containerName);

        _containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        _prefix = prefix?.TrimEnd('/');
    }

    /// <inheritdoc />
    public async Task UploadAsync(
        string topic,
        int partition,
        long baseOffset,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        var blobPath = ObjectStoreKeyFormatter.FormatKey(_prefix, topic, partition, baseOffset);
        var blobClient = _containerClient.GetBlobClient(blobPath);

        await blobClient.UploadAsync(
            BinaryData.FromBytes(data),
            overwrite: true,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<byte[]?> DownloadAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var blobPath = ObjectStoreKeyFormatter.FormatKey(_prefix, topic, partition, baseOffset);
        var blobClient = _containerClient.GetBlobClient(blobPath);

        try
        {
            var response = await blobClient.DownloadContentAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            return response.Value.Content.ToArray();
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
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
        var blobPath = ObjectStoreKeyFormatter.FormatKey(_prefix, topic, partition, baseOffset);
        var blobClient = _containerClient.GetBlobClient(blobPath);

        await blobClient.DeleteIfExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<long>> ListSegmentOffsetsAsync(
        string topic,
        int partition,
        CancellationToken cancellationToken = default)
    {
        var listPrefix = ObjectStoreKeyFormatter.FormatListPrefix(_prefix, topic, partition);
        var offsets = new List<long>();

        await foreach (var blobItem in _containerClient.GetBlobsAsync(
            BlobTraits.None,
            BlobStates.None,
            listPrefix,
            cancellationToken).ConfigureAwait(false))
        {
            var offset = ObjectStoreKeyFormatter.ParseOffsetFromKey(blobItem.Name);
            if (offset.HasValue)
            {
                offsets.Add(offset.Value);
            }
        }

        offsets.Sort();
        return offsets;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        // BlobContainerClient and BlobServiceClient don't require explicit disposal.
    }
}

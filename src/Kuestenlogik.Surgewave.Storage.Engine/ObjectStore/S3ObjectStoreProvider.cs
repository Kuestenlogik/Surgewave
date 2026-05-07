using Amazon.S3;
using Amazon.S3.Model;

namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// AWS S3 implementation of IObjectStoreProvider.
/// Stores segments in S3 with topic/partition/offset-based keys.
/// </summary>
public sealed class S3ObjectStoreProvider : IObjectStoreProvider, IDisposable
{
    private readonly AmazonS3Client _s3Client;
    private readonly string _bucketName;
    private readonly string? _prefix;
    private readonly bool _ownsClient;

    /// <summary>
    /// Create a new S3 object store provider with default credentials from environment.
    /// </summary>
    /// <param name="bucketName">S3 bucket name</param>
    /// <param name="prefix">Optional key prefix (e.g., "surgewave/data")</param>
    /// <param name="client">Optional pre-configured S3 client. If null, creates one from default credentials.</param>
    public S3ObjectStoreProvider(string bucketName, string? prefix, AmazonS3Client? client = null)
    {
        _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        _prefix = prefix?.TrimEnd('/');

        if (client != null)
        {
            _s3Client = client;
            _ownsClient = false;
        }
        else
        {
            _s3Client = new AmazonS3Client();
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
        var key = ObjectStoreKeyFormatter.FormatKey(_prefix, topic, partition, baseOffset);

        using var stream = new MemoryStream(data.ToArray());
        var request = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = key,
            InputStream = stream
        };

        await _s3Client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<byte[]?> DownloadAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var key = ObjectStoreKeyFormatter.FormatKey(_prefix, topic, partition, baseOffset);

        try
        {
            var request = new GetObjectRequest
            {
                BucketName = _bucketName,
                Key = key
            };

            using var response = await _s3Client.GetObjectAsync(request, cancellationToken).ConfigureAwait(false);
            using var memoryStream = new MemoryStream();
            await response.ResponseStream.CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);
            return memoryStream.ToArray();
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
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
        var key = ObjectStoreKeyFormatter.FormatKey(_prefix, topic, partition, baseOffset);

        var request = new DeleteObjectRequest
        {
            BucketName = _bucketName,
            Key = key
        };

        await _s3Client.DeleteObjectAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<long>> ListSegmentOffsetsAsync(
        string topic,
        int partition,
        CancellationToken cancellationToken = default)
    {
        var listPrefix = ObjectStoreKeyFormatter.FormatListPrefix(_prefix, topic, partition);
        var offsets = new List<long>();

        var request = new ListObjectsV2Request
        {
            BucketName = _bucketName,
            Prefix = listPrefix
        };

        ListObjectsV2Response response;
        do
        {
            response = await _s3Client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

            foreach (var obj in response.S3Objects)
            {
                var offset = ObjectStoreKeyFormatter.ParseOffsetFromKey(obj.Key);
                if (offset.HasValue)
                {
                    offsets.Add(offset.Value);
                }
            }

            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated == true);

        offsets.Sort();
        return offsets;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_ownsClient)
        {
            _s3Client.Dispose();
        }
    }
}

using Amazon.S3;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.S3;

/// <summary>
/// Factory for creating S3-backed log segments.
/// </summary>
public sealed class S3LogSegmentFactory : ILogSegmentFactory
{
    private readonly S3StorageEngineFactory _engineFactory;

    public bool IsPersistent => true;

    private S3LogSegmentFactory(S3StorageEngineFactory engineFactory)
    {
        _engineFactory = engineFactory;
    }

    /// <summary>
    /// Create S3 log segment factory with default S3 client.
    /// Uses AWS default credential chain.
    /// </summary>
    public static S3LogSegmentFactory Create(
        string bucketName,
        string prefix = "surgewave",
        ISurgewaveBufferPool? bufferPool = null,
        int batchFlushCount = 100,
        int maxCacheSize = 1000)
    {
        var engineFactory = new S3StorageEngineFactory(
            () => new AmazonS3Client(),
            bucketName,
            prefix,
            bufferPool,
            batchFlushCount,
            maxCacheSize);

        return new S3LogSegmentFactory(engineFactory);
    }

    /// <summary>
    /// Create S3 log segment factory with custom S3 client configuration.
    /// </summary>
    public static S3LogSegmentFactory Create(
        Func<IAmazonS3> clientFactory,
        string bucketName,
        string prefix = "surgewave",
        ISurgewaveBufferPool? bufferPool = null,
        int batchFlushCount = 100,
        int maxCacheSize = 1000)
    {
        var engineFactory = new S3StorageEngineFactory(
            clientFactory,
            bucketName,
            prefix,
            bufferPool,
            batchFlushCount,
            maxCacheSize);

        return new S3LogSegmentFactory(engineFactory);
    }

    /// <summary>
    /// Create S3 log segment factory with specific S3 config for LocalStack/MinIO.
    /// </summary>
    public static S3LogSegmentFactory CreateForLocalStack(
        string endpoint,
        string bucketName,
        string prefix = "surgewave",
        string accessKey = "test",
        string secretKey = "test",
        ISurgewaveBufferPool? bufferPool = null)
    {
        var engineFactory = new S3StorageEngineFactory(
            () => new AmazonS3Client(
                accessKey,
                secretKey,
                new AmazonS3Config
                {
                    ServiceURL = endpoint,
                    ForcePathStyle = true
                }),
            bucketName,
            prefix,
            bufferPool);

        return new S3LogSegmentFactory(engineFactory);
    }

    public ILogSegment CreateSegment(string baseDirectory, long baseOffset, bool createNew, long maxSegmentSize = ILogSegment.DefaultMaxSegmentSize)
    {
#pragma warning disable CA2000 // Engine ownership is transferred to the adapter
        ISurgewaveStorageEngine engine = createNew
            ? _engineFactory.Create(baseDirectory, baseOffset, maxSegmentSize)
            : _engineFactory.Open(baseDirectory, baseOffset);

        return new StorageEngineSegmentAdapter(engine);
#pragma warning restore CA2000
    }
}

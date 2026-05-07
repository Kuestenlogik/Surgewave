using Amazon.S3;
using Kuestenlogik.Surgewave.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.S3;

/// <summary>
/// Factory for creating S3-based storage engine instances.
/// </summary>
public sealed class S3StorageEngineFactory : ISurgewaveStorageEngineFactory
{
    private readonly Func<IAmazonS3> _clientFactory;
    private readonly string _bucketName;
    private readonly string _prefix;
    private readonly ISurgewaveBufferPool? _bufferPool;
    private readonly int _batchFlushCount;
    private readonly int _maxCacheSize;
    private readonly long _defaultMaxSize;

    public S3StorageEngineFactory(
        Func<IAmazonS3> clientFactory,
        string bucketName,
        string prefix = "surgewave",
        ISurgewaveBufferPool? bufferPool = null,
        int batchFlushCount = 100,
        int maxCacheSize = 1000,
        long defaultMaxSize = 1024L * 1024 * 1024)
    {
        _clientFactory = clientFactory;
        _bucketName = bucketName;
        _prefix = prefix;
        _bufferPool = bufferPool;
        _batchFlushCount = batchFlushCount;
        _maxCacheSize = maxCacheSize;
        _defaultMaxSize = defaultMaxSize;
    }

    public ISurgewaveStorageEngine Create(string directory, long baseOffset, long maxSize)
    {
        // For S3, we use the directory as an additional prefix component
        var fullPrefix = string.IsNullOrEmpty(directory)
            ? _prefix
            : $"{_prefix}/{directory.Replace('\\', '/').TrimStart('/')}";

        return new S3StorageEngine(
            _clientFactory(),
            _bucketName,
            fullPrefix,
            baseOffset,
            maxSize,
            createNew: true,
            _bufferPool,
            _batchFlushCount,
            _maxCacheSize);
    }

    public ISurgewaveStorageEngine Open(string directory, long baseOffset)
    {
        var fullPrefix = string.IsNullOrEmpty(directory)
            ? _prefix
            : $"{_prefix}/{directory.Replace('\\', '/').TrimStart('/')}";

        return new S3StorageEngine(
            _clientFactory(),
            _bucketName,
            fullPrefix,
            baseOffset,
            _defaultMaxSize,
            createNew: false,
            _bufferPool,
            _batchFlushCount,
            _maxCacheSize);
    }
}

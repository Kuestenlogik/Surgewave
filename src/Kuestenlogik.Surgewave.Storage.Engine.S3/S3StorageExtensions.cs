using Amazon.S3;
using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.S3;

/// <summary>
/// Extension methods for configuring S3 primary storage on any storage-configurable runtime builder.
/// </summary>
public static class S3StorageExtensions
{
    /// <summary>
    /// Configure S3 as primary storage using default AWS credentials.
    /// Cloud-first storage for serverless deployments.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="bucketName">S3 bucket name.</param>
    /// <param name="prefix">Object key prefix (default: "surgewave").</param>
    public static TBuilder WithS3Storage<TBuilder>(
        this TBuilder builder,
        string bucketName,
        string prefix = "surgewave")
        where TBuilder : IStorageConfigurableBuilder
    {
        builder.UseStorage(() => S3LogSegmentFactory.Create(bucketName, prefix));
        return builder;
    }

    /// <summary>
    /// Configure S3 as primary storage with custom client factory.
    /// </summary>
    public static TBuilder WithS3Storage<TBuilder>(
        this TBuilder builder,
        Func<IAmazonS3> clientFactory,
        string bucketName,
        string prefix = "surgewave",
        ISurgewaveBufferPool? bufferPool = null)
        where TBuilder : IStorageConfigurableBuilder
    {
        builder.UseStorage(() => S3LogSegmentFactory.Create(
            clientFactory, bucketName, prefix, bufferPool));
        return builder;
    }

    /// <summary>
    /// Configure S3 storage for LocalStack or MinIO (local development).
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="endpoint">LocalStack/MinIO endpoint (e.g., "http://localhost:4566").</param>
    /// <param name="bucketName">S3 bucket name.</param>
    /// <param name="prefix">Object key prefix.</param>
    /// <param name="accessKey">Access key (default: "test").</param>
    /// <param name="secretKey">Secret key (default: "test").</param>
    public static TBuilder WithS3StorageLocalStack<TBuilder>(
        this TBuilder builder,
        string endpoint,
        string bucketName,
        string prefix = "surgewave",
        string accessKey = "test",
        string secretKey = "test")
        where TBuilder : IStorageConfigurableBuilder
    {
        builder.UseStorage(() => S3LogSegmentFactory.CreateForLocalStack(
            endpoint, bucketName, prefix, accessKey, secretKey));
        return builder;
    }
}

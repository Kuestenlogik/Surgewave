using Amazon.S3;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.S3;

/// <summary>
/// Extension methods for configuring S3 primary storage on SurgewaveRuntimeBuilder.
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
    public static SurgewaveRuntimeBuilder WithS3Storage(
        this SurgewaveRuntimeBuilder builder,
        string bucketName,
        string prefix = "surgewave")
    {
        return builder.WithStorage(() => S3LogSegmentFactory.Create(bucketName, prefix));
    }

    /// <summary>
    /// Configure S3 as primary storage with custom client factory.
    /// </summary>
    public static SurgewaveRuntimeBuilder WithS3Storage(
        this SurgewaveRuntimeBuilder builder,
        Func<IAmazonS3> clientFactory,
        string bucketName,
        string prefix = "surgewave",
        ISurgewaveBufferPool? bufferPool = null)
    {
        return builder.WithStorage(() => S3LogSegmentFactory.Create(
            clientFactory, bucketName, prefix, bufferPool));
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
    public static SurgewaveRuntimeBuilder WithS3StorageLocalStack(
        this SurgewaveRuntimeBuilder builder,
        string endpoint,
        string bucketName,
        string prefix = "surgewave",
        string accessKey = "test",
        string secretKey = "test")
    {
        return builder.WithStorage(() => S3LogSegmentFactory.CreateForLocalStack(
            endpoint, bucketName, prefix, accessKey, secretKey));
    }
}

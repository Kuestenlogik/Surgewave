using Amazon.S3;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.S3;

/// <summary>
/// Registers S3 storage engines with the StorageRegistry.
/// </summary>
public static class StorageRegistration
{
    private static bool _registered;
    private static readonly object _lock = new();

    /// <summary>
    /// Register S3 storage engine with default configuration.
    /// Requires bucket name to be set via environment variable Surgewave_S3_BUCKET.
    /// </summary>
    public static void Register()
    {
        var bucketName = Environment.GetEnvironmentVariable("Surgewave_S3_BUCKET") ?? "surgewave-data";
        Register(() => new AmazonS3Client(), bucketName);
    }

    /// <summary>
    /// Register S3 storage engine with the global registry.
    /// </summary>
    /// <param name="clientFactory">Factory for creating S3 clients.</param>
    /// <param name="bucketName">S3 bucket name.</param>
    /// <param name="prefix">Object key prefix.</param>
    public static void Register(
        Func<IAmazonS3> clientFactory,
        string bucketName,
        string prefix = "surgewave")
    {
        if (_registered) return;

        lock (_lock)
        {
            if (_registered) return;

            StorageRegistry.Default.Register("s3", () => S3LogSegmentFactory.Create(clientFactory, bucketName, prefix));

            _registered = true;
        }
    }

    /// <summary>
    /// Register S3 storage for LocalStack/MinIO.
    /// </summary>
    public static void RegisterLocalStack(
        string endpoint,
        string bucketName,
        string prefix = "surgewave",
        string accessKey = "test",
        string secretKey = "test")
    {
        if (_registered) return;

        lock (_lock)
        {
            if (_registered) return;

            StorageRegistry.Default.Register("s3", () => S3LogSegmentFactory.CreateForLocalStack(
                endpoint, bucketName, prefix, accessKey, secretKey));

            _registered = true;
        }
    }
}

/// <summary>
/// Module initializer that auto-registers S3 storage when assembly loads.
/// </summary>
file static class ModuleInitializer
{
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void Initialize()
    {
        // Only auto-register if Surgewave_S3_BUCKET is set
        if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("Surgewave_S3_BUCKET")))
        {
            StorageRegistration.Register();
        }
    }
}

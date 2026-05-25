namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Configuration for creating an IObjectStoreProvider via the ObjectStoreProviderFactory.
/// Contains settings for all supported provider types (Local, S3, Azure Blob, GCP).
/// </summary>
public sealed class ObjectStoreProviderConfig
{
    /// <summary>
    /// The type of object store provider to create.
    /// </summary>
    public ObjectStoreProviderType Type { get; init; } = ObjectStoreProviderType.Local;

    // ---- Local ----

    /// <summary>
    /// Local filesystem path for the Local provider. Defaults to "./object-store".
    /// </summary>
    public string? LocalPath { get; init; }

    // ---- S3 / GCP (bucket-based) ----

    /// <summary>
    /// Bucket name for S3 or GCP Cloud Storage.
    /// </summary>
    public string? BucketName { get; init; }

    /// <summary>
    /// AWS region for S3 (e.g., "us-east-1"). Optional; uses default region if not set.
    /// </summary>
    public string? Region { get; init; }

    /// <summary>
    /// AWS access key for S3. Optional; uses default credentials chain if not set.
    /// </summary>
    public string? AccessKey { get; init; }

    /// <summary>
    /// AWS secret key for S3. Optional; uses default credentials chain if not set.
    /// </summary>
    public string? SecretKey { get; init; }

    // ---- Azure ----

    /// <summary>
    /// Azure Storage connection string for Azure Blob Storage.
    /// </summary>
    public string? ConnectionString { get; init; }

    /// <summary>
    /// Azure Blob Storage container name.
    /// </summary>
    public string? ContainerName { get; init; }

    // ---- Common ----

    /// <summary>
    /// Optional key prefix applied to all objects (e.g., "surgewave/data").
    /// Used by S3, Azure Blob, and GCP providers.
    /// </summary>
    public string? Prefix { get; init; }
}

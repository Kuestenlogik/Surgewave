namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Factory for creating IObjectStoreProvider instances from configuration.
/// Supports Local, S3, Azure Blob, and GCP Cloud Storage backends.
/// </summary>
public static class ObjectStoreProviderFactory
{
    /// <summary>
    /// Creates an IObjectStoreProvider based on the given configuration.
    /// </summary>
    /// <param name="config">Provider configuration specifying type and connection details</param>
    /// <returns>A configured IObjectStoreProvider instance</returns>
    /// <exception cref="ArgumentException">Thrown when the provider type is unknown</exception>
    /// <exception cref="ArgumentNullException">Thrown when required configuration values are missing</exception>
    public static IObjectStoreProvider Create(ObjectStoreProviderConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        return config.Type switch
        {
            ObjectStoreProviderType.Local => new LocalFileObjectStoreProvider(config.LocalPath ?? "./object-store"),
            ObjectStoreProviderType.S3 => CreateS3Provider(config),
            ObjectStoreProviderType.AzureBlob => CreateAzureProvider(config),
            ObjectStoreProviderType.Gcp => CreateGcpProvider(config),
            _ => throw new ArgumentException($"Unknown object store provider type: {config.Type}", nameof(config))
        };
    }

    private static S3ObjectStoreProvider CreateS3Provider(ObjectStoreProviderConfig config)
    {
        if (string.IsNullOrEmpty(config.BucketName))
            throw new ArgumentNullException(nameof(config), "BucketName is required for S3 provider");

        Amazon.S3.AmazonS3Client? client = null;

        if (!string.IsNullOrEmpty(config.AccessKey) && !string.IsNullOrEmpty(config.SecretKey))
        {
            var credentials = new Amazon.Runtime.BasicAWSCredentials(config.AccessKey, config.SecretKey);
            var s3Config = new Amazon.S3.AmazonS3Config();

            if (!string.IsNullOrEmpty(config.Region))
            {
                s3Config.RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(config.Region);
            }

            client = new Amazon.S3.AmazonS3Client(credentials, s3Config);
        }
        else if (!string.IsNullOrEmpty(config.Region))
        {
            client = new Amazon.S3.AmazonS3Client(Amazon.RegionEndpoint.GetBySystemName(config.Region));
        }

        return new S3ObjectStoreProvider(config.BucketName, config.Prefix, client);
    }

    private static AzureBlobObjectStoreProvider CreateAzureProvider(ObjectStoreProviderConfig config)
    {
        if (string.IsNullOrEmpty(config.ConnectionString))
            throw new ArgumentNullException(nameof(config), "ConnectionString is required for Azure Blob provider");

        if (string.IsNullOrEmpty(config.ContainerName))
            throw new ArgumentNullException(nameof(config), "ContainerName is required for Azure Blob provider");

        return new AzureBlobObjectStoreProvider(config.ConnectionString, config.ContainerName, config.Prefix);
    }

    private static GcpCloudStorageObjectStoreProvider CreateGcpProvider(ObjectStoreProviderConfig config)
    {
        if (string.IsNullOrEmpty(config.BucketName))
            throw new ArgumentNullException(nameof(config), "BucketName is required for GCP Cloud Storage provider");

        return new GcpCloudStorageObjectStoreProvider(config.BucketName, config.Prefix);
    }
}

namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Supported object store provider types for cloud and local storage backends.
/// </summary>
public enum ObjectStoreProviderType
{
    /// <summary>Local filesystem storage (development/testing/single-node).</summary>
    Local,

    /// <summary>Amazon S3 storage.</summary>
    S3,

    /// <summary>Azure Blob Storage.</summary>
    AzureBlob,

    /// <summary>Google Cloud Storage.</summary>
    Gcp
}

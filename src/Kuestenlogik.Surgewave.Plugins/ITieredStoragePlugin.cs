namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Plugin that provides a tiered storage provider (S3, Azure Blob, GCP Cloud Storage).
/// Tiered storage plugins register their provider factory with the TieredStorageManager
/// so that segments can be offloaded to remote object storage.
/// </summary>
public interface ITieredStoragePlugin : IPlugin
{
    /// <summary>
    /// The provider name (e.g., "s3", "azure", "gcp"). Must match the configuration value.
    /// </summary>
    string ProviderName { get; }

    /// <summary>
    /// Registers this provider's factory with the tiered storage system.
    /// Called during broker startup before tiered storage is initialized.
    /// </summary>
    void Register();
}

namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Azure Blob Storage configuration.
/// </summary>
public sealed class AzureOptions
{
    /// <summary>
    /// Azure Storage connection string.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Container name.
    /// </summary>
    public string? ContainerName { get; set; }

    /// <summary>
    /// Blob prefix.
    /// </summary>
    public string? Prefix { get; set; }
}

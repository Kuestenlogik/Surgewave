namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Google Cloud Storage configuration.
/// </summary>
public sealed class GcpOptions
{
    /// <summary>
    /// GCS bucket name.
    /// </summary>
    public string? BucketName { get; set; }

    /// <summary>
    /// Object key prefix.
    /// </summary>
    public string? Prefix { get; set; }
}

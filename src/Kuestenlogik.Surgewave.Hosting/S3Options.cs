namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// S3 storage configuration.
/// </summary>
public sealed class S3Options
{
    /// <summary>
    /// S3 bucket name.
    /// </summary>
    public string? BucketName { get; set; }

    /// <summary>
    /// AWS region.
    /// </summary>
    public string? Region { get; set; }

    /// <summary>
    /// Object key prefix.
    /// </summary>
    public string? Prefix { get; set; }

    /// <summary>
    /// Custom endpoint URL (for MinIO, LocalStack, etc.).
    /// </summary>
    public string? EndpointUrl { get; set; }
}

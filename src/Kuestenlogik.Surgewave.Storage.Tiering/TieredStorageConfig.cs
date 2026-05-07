using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Storage.Tiering;

/// <summary>
/// Configuration for tiered storage
/// </summary>
public sealed record TieredStorageConfig : IValidatableConfig
{
    /// <summary>
    /// Enable tiered storage
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Storage provider type: "local", "azure", "s3", "gcp"
    /// </summary>
    [RegularExpression("^(local|azure|s3|gcp)$",
        ErrorMessage = "Provider must be one of: local, azure, s3, gcp.")]
    public string Provider { get; init; } = "local";

    /// <summary>
    /// Path for local filesystem provider
    /// </summary>
    [Required]
    [MinLength(1)]
    public string LocalPath { get; init; } = "./tiered-storage";

    /// <summary>
    /// Azure Storage connection string
    /// </summary>
    public string? AzureConnectionString { get; init; }

    /// <summary>
    /// Azure Storage container name
    /// </summary>
    public string AzureContainerName { get; init; } = "surgewave-tiered";

    /// <summary>
    /// S3 bucket name
    /// </summary>
    public string? S3BucketName { get; init; }

    /// <summary>
    /// S3 region (optional)
    /// </summary>
    public string? S3Region { get; init; }

    /// <summary>
    /// GCP bucket name
    /// </summary>
    public string? GcpBucketName { get; init; }

    /// <summary>
    /// Prefix for all remote objects
    /// </summary>
    public string Prefix { get; init; } = "";

    /// <summary>
    /// How long to keep segments locally after tiering (in hours).
    /// After this time, local copies are deleted.
    /// Set to -1 to keep local copies indefinitely.
    /// </summary>
    public int LocalRetentionHours { get; init; } = 24;

    /// <summary>
    /// How long to keep segments in remote storage (in hours).
    /// Set to -1 for indefinite retention.
    /// </summary>
    public int RemoteRetentionHours { get; init; } = -1;

    /// <summary>
    /// Minimum segment age (in hours) before tiering.
    /// Prevents tiering of very recent segments.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int TieringLagHours { get; init; } = 1;

    /// <summary>
    /// Minimum segment size (in bytes) to tier.
    /// Very small segments may not be worth tiering.
    /// </summary>
    [Range(0, long.MaxValue)]
    public long MinSegmentSizeBytes { get; init; } = 1024 * 1024; // 1 MB

    /// <summary>
    /// Maximum size of local cache for downloaded remote segments (in bytes).
    /// When exceeded, LRU eviction is triggered.
    /// </summary>
    [Range(0, long.MaxValue)]
    public long LocalCacheSizeBytes { get; init; } = 1024L * 1024 * 1024; // 1 GB

    /// <summary>
    /// Directory for caching downloaded remote segments
    /// </summary>
    [Required]
    [MinLength(1)]
    public string LocalCachePath { get; init; } = "./tiered-cache";

    /// <summary>
    /// Whether to delete local segments after successful upload to remote
    /// </summary>
    public bool DeleteAfterUpload { get; init; } = true;

    /// <summary>
    /// Background tiering interval (in seconds)
    /// </summary>
    [Range(1, int.MaxValue)]
    public int TieringIntervalSeconds { get; init; } = 300; // 5 minutes

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (Enabled)
        {
            switch (Provider)
            {
                case "azure" when string.IsNullOrWhiteSpace(AzureConnectionString):
                    errors.Add($"{nameof(AzureConnectionString)}: required when Provider is 'azure'.");
                    break;
                case "s3" when string.IsNullOrWhiteSpace(S3BucketName):
                    errors.Add($"{nameof(S3BucketName)}: required when Provider is 's3'.");
                    break;
                case "gcp" when string.IsNullOrWhiteSpace(GcpBucketName):
                    errors.Add($"{nameof(GcpBucketName)}: required when Provider is 'gcp'.");
                    break;
            }
        }

        return errors;
    }
}

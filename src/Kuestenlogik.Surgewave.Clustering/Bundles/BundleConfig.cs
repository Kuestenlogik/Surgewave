using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Clustering.Bundles;

/// <summary>
/// Configuration for bundle management including split thresholds and limits.
/// </summary>
public sealed class BundleConfig : IValidatableConfig
{
    /// <summary>
    /// Number of bundles to create when the bundle manager is initialized.
    /// The full uint32 hash range is divided equally among this many bundles.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int InitialBundleCount { get; init; } = 4;

    /// <summary>
    /// Maximum number of bundles allowed per namespace.
    /// Prevents unbounded splitting.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxBundlesPerNamespace { get; init; } = 128;

    /// <summary>
    /// Maximum number of topics a single bundle should host before considering a split.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxTopicsPerBundle { get; init; } = 1000;

    /// <summary>
    /// Maximum message rate (messages/second) a bundle should handle before a split is recommended.
    /// </summary>
    [Range(1, long.MaxValue)]
    public long MaxMessageRatePerBundle { get; init; } = 30_000;

    /// <summary>
    /// Maximum bandwidth in megabytes per second a bundle should handle before a split is recommended.
    /// </summary>
    [Range(1, long.MaxValue)]
    public long MaxBandwidthMbPerBundle { get; init; } = 100;

    /// <summary>
    /// Whether bundles should be automatically split when thresholds are exceeded.
    /// </summary>
    public bool AutoSplitEnabled { get; init; } = true;

    /// <summary>
    /// Whether a bundle should be automatically unloaded after being split,
    /// making the two halves available for reassignment to balance load.
    /// </summary>
    public bool AutoUnloadAfterSplit { get; init; } = true;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));
        if (InitialBundleCount > MaxBundlesPerNamespace)
            errors.Add($"{nameof(InitialBundleCount)} must not exceed {nameof(MaxBundlesPerNamespace)}.");
        return errors;
    }
}

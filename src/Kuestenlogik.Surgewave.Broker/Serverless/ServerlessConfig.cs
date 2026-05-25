using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// Configuration for serverless scaling mode.
/// Controls idle timeouts, drain behavior, cold start optimization, and auto-scaling thresholds.
/// </summary>
public sealed class ServerlessConfig : IValidatableConfig
{
    /// <summary>
    /// Enable serverless scaling mode. Default: false.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// How long a broker can be idle before it becomes eligible for scale-down.
    /// Default: 5 minutes.
    /// </summary>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum time to wait for drain to complete before force-terminating.
    /// Default: 60 seconds.
    /// </summary>
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Maximum allowed cold start duration before timeout.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan ColdStartTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Minimum number of broker instances. Set to 0 for true scale-to-zero.
    /// Default: 0.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int MinBrokers { get; set; }

    /// <summary>
    /// Maximum number of broker instances.
    /// Default: 10.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxBrokers { get; set; } = 10;

    /// <summary>
    /// CPU/throughput threshold (percent) above which a new broker should be added.
    /// Default: 70%.
    /// </summary>
    [Range(0.0, 100.0)]
    public double ScaleUpThresholdPercent { get; set; } = 70.0;

    /// <summary>
    /// CPU/throughput threshold (percent) below which a broker can be removed.
    /// Default: 20%.
    /// </summary>
    [Range(0.0, 100.0)]
    public double ScaleDownThresholdPercent { get; set; } = 20.0;

    /// <summary>
    /// Stabilization window: minimum time between scaling decisions to prevent flapping.
    /// Default: 2 minutes.
    /// </summary>
    public TimeSpan StabilizationWindow { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Number of hot partitions to pre-fetch during cold start warming phase.
    /// Default: 10.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int WarmupPartitions { get; set; } = 10;

    /// <summary>
    /// Preferred object store providers in priority order, e.g. ["s3", "azure", "gcp"].
    /// </summary>
    public string[] PreferredObjectStoreProviders { get; set; } = [];

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));
        if (MinBrokers > MaxBrokers)
            errors.Add($"{nameof(MinBrokers)} must not exceed {nameof(MaxBrokers)}.");
        if (ScaleDownThresholdPercent >= ScaleUpThresholdPercent)
            errors.Add($"{nameof(ScaleDownThresholdPercent)} must be less than {nameof(ScaleUpThresholdPercent)}.");
        return errors;
    }
}

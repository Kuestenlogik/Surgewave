using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// Configuration for the Cruise Control (auto-balance) system.
/// Controls how frequently the cluster balance is analyzed, the operating mode,
/// throttling rate, and cooldown between rebalance operations.
/// </summary>
public sealed class CruiseControlConfig : IValidatableConfig
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "Surgewave:CruiseControl";

    /// <summary>
    /// Whether Cruise Control is enabled. Default: false (opt-in).
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Interval in seconds between balance analysis cycles.
    /// Default: 300 (5 minutes).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int AnalysisIntervalSeconds { get; set; } = 300;

    /// <summary>
    /// The operating mode. Controls whether rebalance plans are merely suggested or automatically executed.
    /// </summary>
    public CruiseControlMode Mode { get; set; } = CruiseControlMode.SuggestOnly;

    /// <summary>
    /// Balance goals that define what "balanced" means for each metric.
    /// </summary>
    public BalanceGoals Goals { get; set; } = new();

    /// <summary>
    /// Throttle rate in bytes per second for data replication during rebalance operations.
    /// Default: 50 MB/s.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int ThrottleRateBytesPerSec { get; set; } = 50_000_000;

    /// <summary>
    /// Minimum time in minutes between automatic rebalance operations to prevent thrashing.
    /// Default: 30 minutes.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int CooldownMinutes { get; set; } = 30;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

/// <summary>
/// Determines how Cruise Control handles detected imbalances.
/// </summary>
public enum CruiseControlMode
{
    /// <summary>
    /// Only suggest rebalance plans. No automatic execution.
    /// </summary>
    SuggestOnly,

    /// <summary>
    /// Automatically execute rebalance plans when imbalance is detected (respecting cooldown).
    /// </summary>
    AutoRebalance
}

namespace Kuestenlogik.Surgewave.Broker.AutoTuning;

/// <summary>
/// Configuration for the adaptive auto-tuning system.
/// </summary>
public sealed class AutoTuningConfig
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "Surgewave:AutoTuning";

    /// <summary>
    /// Whether auto-tuning is enabled. Default: false (opt-in).
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// The auto-tuning mode. Controls whether recommendations are merely suggested or automatically applied.
    /// </summary>
    public AutoTuningMode Mode { get; set; } = AutoTuningMode.SuggestOnly;

    /// <summary>
    /// Interval in seconds between metric analysis cycles.
    /// </summary>
    public int AnalysisIntervalSeconds { get; set; } = 30;

    /// <summary>
    /// List of rule IDs to disable. Disabled rules are skipped during analysis.
    /// </summary>
    public List<string> DisabledRules { get; set; } = [];
}

/// <summary>
/// Determines how auto-tuning recommendations are handled.
/// </summary>
public enum AutoTuningMode
{
    /// <summary>
    /// Only suggest changes. No configuration is modified automatically.
    /// </summary>
    SuggestOnly,

    /// <summary>
    /// Automatically apply all recommendations.
    /// </summary>
    AutoApply,

    /// <summary>
    /// Auto-apply safe changes, suggest risky ones.
    /// </summary>
    Mixed
}

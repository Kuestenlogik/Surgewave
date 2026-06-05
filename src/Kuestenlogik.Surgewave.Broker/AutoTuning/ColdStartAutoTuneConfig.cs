namespace Kuestenlogik.Surgewave.Broker.AutoTuning;

/// <summary>
/// Configuration for the cold-start auto-tuning subsystem. Lives under
/// <c>Surgewave:AutoTune:ColdStart</c> in <c>appsettings.json</c> /
/// environment variables.
/// </summary>
/// <remarks>
/// Unlike the broader <see cref="AutoTuningConfig"/> (which runs a static
/// rule set every <see cref="AutoTuningConfig.AnalysisIntervalSeconds"/>),
/// the cold-start service observes *real* traffic for one observation
/// window — typically the first 24 h after startup — and only then emits
/// recommendations based on the measured workload shape.
/// </remarks>
public sealed class ColdStartAutoTuneConfig
{
    /// <summary>Configuration section name (<c>Surgewave:AutoTune:ColdStart</c>).</summary>
    public const string SectionName = "Surgewave:AutoTune:ColdStart";

    /// <summary>Whether the cold-start service runs at all. Opt-in.</summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Length of the observation window. After this much wall-clock time
    /// elapses, the profile is closed, recommendations are computed and
    /// either suggested or auto-applied. Default 24 h.
    /// </summary>
    public TimeSpan ObservationWindow { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// Service tick interval — how often the background loop checks whether
    /// the observation window has elapsed. Defaults to 60 s; reduce in
    /// tests by setting this together with <see cref="ObservationWindow"/>.
    /// </summary>
    public TimeSpan CheckInterval { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// When <c>true</c>, recommendations are applied directly via
    /// <see cref="DynamicBrokerConfig"/>. When <c>false</c> (default), they
    /// are only persisted to <see cref="AutoTunedJsonPath"/> for the
    /// operator to review.
    /// </summary>
    public bool AutoApply { get; set; }

    /// <summary>
    /// Path of the JSON audit-trail file the service writes after each
    /// recommendation cycle. Relative paths resolve against the broker's
    /// working directory. Default: <c>auto-tuned.json</c>.
    /// </summary>
    public string AutoTunedJsonPath { get; set; } = "auto-tuned.json";
}

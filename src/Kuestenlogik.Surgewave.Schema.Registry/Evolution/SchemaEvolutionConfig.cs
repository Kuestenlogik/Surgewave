namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// Configuration for the AI-assisted schema evolution analyzer.
/// Bound from appsettings.json under "Surgewave:SchemaEvolution".
/// </summary>
public sealed class SchemaEvolutionConfig
{
    /// <summary>
    /// Enable schema evolution monitoring and analysis.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Interval in seconds between checks for new schema versions.
    /// </summary>
    public int CheckIntervalSeconds { get; set; } = 60;

    /// <summary>
    /// Automatically generate migration code when a new schema version is detected.
    /// </summary>
    public bool AutoGenerateCode { get; set; } = true;

    /// <summary>
    /// Send a notification to the Assistant when a schema change is detected.
    /// </summary>
    public bool NotifyAssistant { get; set; } = true;

    /// <summary>
    /// Default C# namespace for generated model classes.
    /// </summary>
    public string DefaultNamespace { get; set; } = "Surgewave.Models";
}

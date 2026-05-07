namespace Kuestenlogik.Surgewave.Schema.Registry.Inference;

/// <summary>
/// Configuration for the live schema inference system.
/// </summary>
public sealed class SchemaInferenceConfig
{
    /// <summary>
    /// Whether schema inference is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Number of messages to sample per topic for schema inference.
    /// </summary>
    public int SampleSize { get; set; } = 100;

    /// <summary>
    /// How often (in seconds) to refresh inferred schemas.
    /// </summary>
    public int RefreshIntervalSeconds { get; set; } = 60;

    /// <summary>
    /// Topic name patterns to exclude from inference (supports * wildcard).
    /// Internal topics (prefixed with __) are excluded by default.
    /// </summary>
    public List<string> ExcludedTopics { get; set; } = ["__*"];

    /// <summary>
    /// Whether to automatically register inferred schemas in the Schema Registry.
    /// When true, schemas are registered with subject name "{topic}-inferred-value".
    /// </summary>
    public bool AutoRegister { get; set; } = true;
}

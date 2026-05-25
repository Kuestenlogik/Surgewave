namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Configuration for the Schema Registry.
/// </summary>
public sealed class SchemaRegistryConfig
{
    /// <summary>
    /// Path to store schema data.
    /// </summary>
    public string DataPath { get; init; } = "./data/schemas";

    /// <summary>
    /// Default compatibility mode for new subjects.
    /// </summary>
    public CompatibilityMode DefaultCompatibility { get; init; } = CompatibilityMode.Backward;
}

namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Connect cluster information.
/// </summary>
public sealed class ClusterInfoResponse
{
    /// <summary>
    /// Connect version.
    /// </summary>
    public required string Version { get; init; }

    /// <summary>
    /// Git commit hash.
    /// </summary>
    public required string Commit { get; init; }

    /// <summary>
    /// Kafka cluster ID.
    /// </summary>
    public required string KafkaClusterId { get; init; }
}

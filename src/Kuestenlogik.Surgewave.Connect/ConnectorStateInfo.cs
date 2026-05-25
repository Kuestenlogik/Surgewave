namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Connector state information.
/// </summary>
public sealed class ConnectorStateInfo
{
    /// <summary>
    /// Current state of the connector.
    /// </summary>
    public required string State { get; init; }

    /// <summary>
    /// Worker ID running this connector.
    /// </summary>
    public required string WorkerId { get; init; }

    /// <summary>
    /// Error trace if the connector has failed.
    /// </summary>
    public string? Trace { get; init; }
}

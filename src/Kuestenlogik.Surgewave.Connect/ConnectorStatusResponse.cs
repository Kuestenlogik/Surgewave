namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Connector status response.
/// </summary>
public sealed class ConnectorStatusResponse
{
    /// <summary>
    /// Name of the connector.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Connector state information.
    /// </summary>
    public required ConnectorStateInfo Connector { get; init; }

    /// <summary>
    /// Task state information.
    /// </summary>
    public required List<TaskStateInfo> Tasks { get; init; }

    /// <summary>
    /// Type of connector (source or sink).
    /// </summary>
    public required string Type { get; init; }
}

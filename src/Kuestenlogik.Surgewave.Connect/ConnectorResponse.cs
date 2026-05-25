namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Response with connector information.
/// </summary>
public sealed class ConnectorResponse
{
    /// <summary>
    /// Name of the connector.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Connector configuration.
    /// </summary>
    public required IDictionary<string, string> Config { get; init; }

    /// <summary>
    /// List of tasks for this connector.
    /// </summary>
    public required List<TaskId> Tasks { get; init; }

    /// <summary>
    /// Type of connector (source or sink).
    /// </summary>
    public required string Type { get; init; }
}

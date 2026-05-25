namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Information about a connector.
/// </summary>
public sealed class ConnectorInfo
{
    public required string Name { get; init; }
    public required string Type { get; init; }
    public required string State { get; init; }
    public required string WorkerId { get; init; }
    public required IDictionary<string, string> Config { get; init; }
    public required List<TaskStatus> Tasks { get; init; }
}

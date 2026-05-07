namespace Kuestenlogik.Surgewave.Client.Native.Operations.Connect;

/// <summary>
/// Connector status.
/// </summary>
public record ConnectorStatus(
    string Name,
    string Type,
    string State,
    string WorkerId,
    IReadOnlyList<ConnectorTaskStatus> Tasks);

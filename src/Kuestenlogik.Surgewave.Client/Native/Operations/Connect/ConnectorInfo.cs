namespace Kuestenlogik.Surgewave.Client.Native.Operations.Connect;

/// <summary>
/// Connector information.
/// </summary>
public record ConnectorInfo(
    string Name,
    string Type,
    string State,
    string WorkerId,
    Dictionary<string, string> Config,
    IReadOnlyList<ConnectorTaskStatus> Tasks);

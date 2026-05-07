namespace Kuestenlogik.Surgewave.Client.Native.Operations.Connect;

/// <summary>
/// Connector task status.
/// </summary>
public record ConnectorTaskStatus(int Id, string State, string WorkerId, string? Trace);

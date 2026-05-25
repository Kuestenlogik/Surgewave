namespace Kuestenlogik.Surgewave.Connect.Distributed;

/// <summary>
/// Command message published to the config topic to control a remote connector.
/// Supports "stop", "pause", and "resume" commands.
/// </summary>
public sealed class RemoteTaskCommand
{
    /// <summary>
    /// Name of the connector to control.
    /// </summary>
    public required string ConnectorName { get; init; }

    /// <summary>
    /// ID of the worker running this connector.
    /// </summary>
    public required string WorkerId { get; init; }

    /// <summary>
    /// Command to execute: "stop", "pause", "resume".
    /// </summary>
    public required string Command { get; init; }

    /// <summary>
    /// Timestamp when the command was issued.
    /// </summary>
    public long Timestamp { get; init; }
}

namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task identifier.
/// </summary>
public sealed class TaskId
{
    /// <summary>
    /// Name of the connector.
    /// </summary>
    public required string Connector { get; init; }

    /// <summary>
    /// Task ID within the connector.
    /// </summary>
    public required int Task { get; init; }
}

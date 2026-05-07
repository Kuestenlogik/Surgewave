namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task information.
/// </summary>
public sealed class TaskInfo
{
    /// <summary>
    /// Task identifier.
    /// </summary>
    public required TaskId Id { get; init; }

    /// <summary>
    /// Task configuration.
    /// </summary>
    public required Dictionary<string, string> Config { get; init; }
}

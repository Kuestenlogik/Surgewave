namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Debug state for a pipeline, including paused nodes and active breakpoints.
/// </summary>
public record DebugState
{
    public Dictionary<string, PausedNodeInfo> PausedNodes { get; init; } = new();
    public HashSet<string> Breakpoints { get; init; } = [];
}

/// <summary>
/// Information about a node paused at a breakpoint.
/// </summary>
public record PausedNodeInfo
{
    public required string NodeId { get; init; }
    public string? RecordKey { get; init; }
    public string? RecordValue { get; init; }
    public DateTimeOffset PausedAt { get; init; }
    public int QueueDepth { get; init; }
}

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

using System.Collections.Concurrent;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Pipeline debugger service that manages breakpoints and pause states.
/// Allows single-stepping through records and inspecting paused data.
/// </summary>
public sealed class PipelineDebugger
{
    private readonly ConcurrentDictionary<string, HashSet<string>> _breakpoints = new();
    private readonly ConcurrentDictionary<string, DebugPauseState> _pausedNodes = new();

    /// <summary>
    /// Set a breakpoint on a node within a pipeline.
    /// </summary>
    public void SetBreakpoint(string pipelineId, string nodeId)
    {
        var breakpoints = _breakpoints.GetOrAdd(pipelineId, _ => []);
        lock (breakpoints)
        {
            breakpoints.Add(nodeId);
        }
    }

    /// <summary>
    /// Remove a breakpoint from a node.
    /// </summary>
    public void RemoveBreakpoint(string pipelineId, string nodeId)
    {
        if (_breakpoints.TryGetValue(pipelineId, out var breakpoints))
        {
            lock (breakpoints)
            {
                breakpoints.Remove(nodeId);
            }
        }
    }

    /// <summary>
    /// Get all breakpoints for a pipeline.
    /// </summary>
    public HashSet<string> GetBreakpoints(string pipelineId)
    {
        if (_breakpoints.TryGetValue(pipelineId, out var breakpoints))
        {
            lock (breakpoints)
            {
                return [.. breakpoints];
            }
        }
        return [];
    }

    /// <summary>
    /// Called when a record arrives at a node. If a breakpoint is set,
    /// returns a ManualResetEventSlim gate that the caller should wait on.
    /// </summary>
    public ManualResetEventSlim? NotifyRecordArrived(string pipelineId, string nodeId, SinkRecord record)
    {
        if (!_breakpoints.TryGetValue(pipelineId, out var breakpoints))
            return null;

        bool hasBreakpoint;
        lock (breakpoints)
        {
            hasBreakpoint = breakpoints.Contains(nodeId);
        }

        if (!hasBreakpoint)
            return null;

        var key = $"{pipelineId}:{nodeId}";
        var gate = new ManualResetEventSlim(false);

        var keyStr = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null;
        var valueStr = record.Value != null ? TruncateValue(Encoding.UTF8.GetString(record.Value)) : null;

        var state = new DebugPauseState(keyStr, valueStr, DateTimeOffset.UtcNow, gate, false);
        _pausedNodes.AddOrUpdate(key, state, (_, existing) =>
        {
            existing.Gate.Set(); // Release any previously waiting gate
            return state;
        });

        return gate;
    }

    /// <summary>
    /// Step to the next record on a specific node (release gate, re-pause on next).
    /// </summary>
    public void StepNext(string pipelineId, string nodeId)
    {
        var key = $"{pipelineId}:{nodeId}";
        if (_pausedNodes.TryGetValue(key, out var state))
        {
            // Mark step mode so the node re-pauses on next record
            _pausedNodes[key] = state with { StepMode = true };
            state.Gate.Set();
        }
    }

    /// <summary>
    /// Resume a specific node (release gate and remove breakpoint pause).
    /// </summary>
    public void ResumeNode(string pipelineId, string nodeId)
    {
        var key = $"{pipelineId}:{nodeId}";
        if (_pausedNodes.TryRemove(key, out var state))
        {
            state.Gate.Set();
        }
    }

    /// <summary>
    /// Resume all paused nodes in a pipeline.
    /// </summary>
    public void ResumeAll(string pipelineId)
    {
        var prefix = $"{pipelineId}:";
        foreach (var kvp in _pausedNodes)
        {
            if (kvp.Key.StartsWith(prefix, StringComparison.Ordinal))
            {
                if (_pausedNodes.TryRemove(kvp.Key, out var state))
                {
                    state.Gate.Set();
                }
            }
        }
    }

    /// <summary>
    /// Get the debug state for a pipeline.
    /// </summary>
    public DebugState GetDebugState(string pipelineId)
    {
        var breakpoints = GetBreakpoints(pipelineId);
        var pausedNodes = new Dictionary<string, PausedNodeInfo>();

        var prefix = $"{pipelineId}:";
        foreach (var kvp in _pausedNodes)
        {
            if (kvp.Key.StartsWith(prefix, StringComparison.Ordinal))
            {
                var nodeId = kvp.Key[prefix.Length..];
                pausedNodes[nodeId] = new PausedNodeInfo
                {
                    NodeId = nodeId,
                    RecordKey = kvp.Value.RecordKey,
                    RecordValue = kvp.Value.RecordValue,
                    PausedAt = kvp.Value.PausedAt,
                    QueueDepth = 0 // Could track queue depth
                };
            }
        }

        return new DebugState
        {
            Breakpoints = breakpoints,
            PausedNodes = pausedNodes
        };
    }

    /// <summary>
    /// Clear all debug state for a pipeline.
    /// </summary>
    public void Clear(string pipelineId)
    {
        _breakpoints.TryRemove(pipelineId, out _);
        ResumeAll(pipelineId);
    }

    private static string TruncateValue(string value, int maxLength = 2048)
    {
        return value.Length > maxLength ? value[..maxLength] + "..." : value;
    }
}

/// <summary>
/// State of a node paused at a breakpoint.
/// </summary>
public record DebugPauseState(string? RecordKey, string? RecordValue, DateTimeOffset PausedAt, ManualResetEventSlim Gate, bool StepMode);

/// <summary>
/// Debug state for a pipeline.
/// </summary>
public record DebugState
{
    public Dictionary<string, PausedNodeInfo> PausedNodes { get; init; } = new();
    public HashSet<string> Breakpoints { get; init; } = [];
}

/// <summary>
/// Information about a paused node.
/// </summary>
public record PausedNodeInfo
{
    public required string NodeId { get; init; }
    public string? RecordKey { get; init; }
    public string? RecordValue { get; init; }
    public DateTimeOffset PausedAt { get; init; }
    public int QueueDepth { get; init; }
}

/// <summary>
/// Request to set breakpoints.
/// </summary>
public record SetBreakpointsRequest
{
    public required string[] NodeIds { get; init; }
}

using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Connect.Distributed;

/// <summary>
/// Tracks which worker owns which connector tasks for pipeline orchestration.
/// Thread-safe for concurrent access from orchestrator and heartbeat processing.
/// </summary>
public sealed class TaskAssignmentTracker
{
    private readonly ConcurrentDictionary<string, WorkerTaskAssignment> _assignments = new(StringComparer.Ordinal);

    /// <summary>
    /// Records that a connector was assigned to a specific worker.
    /// </summary>
    public void TrackAssignment(string connectorName, string workerId, string pipelineId, string nodeId)
    {
        _assignments[connectorName] = new WorkerTaskAssignment
        {
            ConnectorName = connectorName,
            WorkerId = workerId,
            PipelineId = pipelineId,
            NodeId = nodeId,
            AssignedAt = DateTimeOffset.UtcNow
        };
    }

    /// <summary>
    /// Removes the assignment for a connector.
    /// </summary>
    public bool RemoveAssignment(string connectorName)
    {
        return _assignments.TryRemove(connectorName, out _);
    }

    /// <summary>
    /// Gets the assignment for a connector, if any.
    /// </summary>
    public WorkerTaskAssignment? GetAssignment(string connectorName)
    {
        _assignments.TryGetValue(connectorName, out var assignment);
        return assignment;
    }

    /// <summary>
    /// Gets all assignments for a specific worker.
    /// </summary>
    public IReadOnlyList<WorkerTaskAssignment> GetAssignmentsForWorker(string workerId)
    {
        return _assignments.Values
            .Where(a => a.WorkerId.Equals(workerId, StringComparison.Ordinal))
            .ToList();
    }

    /// <summary>
    /// Gets all assignments for a specific pipeline.
    /// </summary>
    public IReadOnlyList<WorkerTaskAssignment> GetAssignmentsForPipeline(string pipelineId)
    {
        return _assignments.Values
            .Where(a => a.PipelineId.Equals(pipelineId, StringComparison.Ordinal))
            .ToList();
    }

    /// <summary>
    /// Gets the worker ID that owns a connector, or null if locally owned.
    /// </summary>
    public string? GetOwningWorker(string connectorName)
    {
        return _assignments.TryGetValue(connectorName, out var assignment) ? assignment.WorkerId : null;
    }

    /// <summary>
    /// Gets all current assignments.
    /// </summary>
    public IReadOnlyList<WorkerTaskAssignment> GetAllAssignments()
    {
        return _assignments.Values.ToList();
    }

    /// <summary>
    /// Removes all assignments for a specific worker (e.g., when worker disconnects).
    /// Returns the removed assignments for reassignment.
    /// </summary>
    public IReadOnlyList<WorkerTaskAssignment> RemoveWorkerAssignments(string workerId)
    {
        var removed = new List<WorkerTaskAssignment>();
        foreach (var (key, assignment) in _assignments)
        {
            if (assignment.WorkerId.Equals(workerId, StringComparison.Ordinal))
            {
                if (_assignments.TryRemove(key, out var removedAssignment))
                {
                    removed.Add(removedAssignment);
                }
            }
        }
        return removed;
    }
}

/// <summary>
/// Represents the assignment of a connector to a worker within a pipeline context.
/// </summary>
public sealed class WorkerTaskAssignment
{
    /// <summary>
    /// Name of the connector instance.
    /// </summary>
    public required string ConnectorName { get; init; }

    /// <summary>
    /// ID of the worker running this connector.
    /// </summary>
    public required string WorkerId { get; init; }

    /// <summary>
    /// ID of the pipeline this connector belongs to.
    /// </summary>
    public required string PipelineId { get; init; }

    /// <summary>
    /// ID of the node within the pipeline.
    /// </summary>
    public required string NodeId { get; init; }

    /// <summary>
    /// When the assignment was created.
    /// </summary>
    public DateTimeOffset AssignedAt { get; init; }

    /// <summary>
    /// Whether this is a remote assignment (vs. local).
    /// </summary>
    public bool IsRemote { get; init; }
}

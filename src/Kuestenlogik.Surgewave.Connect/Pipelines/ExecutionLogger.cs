using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Logs pipeline execution events and metrics.
/// Thread-safe for concurrent access from multiple connector tasks.
/// </summary>
public sealed class ExecutionLogger
{
    private readonly ExecutionStore _store;
    private readonly ILogger<ExecutionLogger> _logger;
    private readonly ConcurrentDictionary<string, ExecutionContext> _activeExecutions = new();

    public ExecutionLogger(ExecutionStore store, ILogger<ExecutionLogger> logger)
    {
        _store = store;
        _logger = logger;
    }

    /// <summary>
    /// Start tracking a pipeline execution.
    /// </summary>
    public async Task<string> StartExecutionAsync(
        string pipelineId,
        string pipelineName,
        IEnumerable<PipelineNode> nodes,
        string triggerType = "manual",
        Dictionary<string, string>? triggerMetadata = null,
        CancellationToken cancellationToken = default)
    {
        var execution = await _store.CreateAsync(pipelineId, pipelineName, triggerType, triggerMetadata, cancellationToken);

        var context = new ExecutionContext
        {
            ExecutionId = execution.Id,
            PipelineId = pipelineId,
            StartedAt = execution.StartedAt
        };

        // Initialize node contexts
        foreach (var node in nodes)
        {
            context.NodeContexts[node.Id] = new NodeExecutionContext
            {
                NodeId = node.Id,
                NodeLabel = node.Label,
                ConnectorType = node.ConnectorType,
                StartedAt = DateTimeOffset.UtcNow
            };
        }

        _activeExecutions[execution.Id] = context;
        _logger.LogInformation("Started execution {ExecutionId} for pipeline {PipelineId}", execution.Id, pipelineId);

        return execution.Id;
    }

    /// <summary>
    /// Record records processed by a node.
    /// </summary>
    public void RecordInput(string executionId, string nodeId, int count, IEnumerable<SampleRecord>? samples = null)
    {
        if (!_activeExecutions.TryGetValue(executionId, out var context)) return;
        if (!context.NodeContexts.TryGetValue(nodeId, out var nodeContext)) return;

        Interlocked.Add(ref nodeContext.RecordsIn, count);

        if (samples != null && nodeContext.SampleInputs.Count < 5)
        {
            foreach (var sample in samples.Take(5 - nodeContext.SampleInputs.Count))
            {
                nodeContext.SampleInputs.Add(sample);
            }
        }
    }

    /// <summary>
    /// Record records output by a node.
    /// </summary>
    public void RecordOutput(string executionId, string nodeId, int count, IEnumerable<SampleRecord>? samples = null)
    {
        if (!_activeExecutions.TryGetValue(executionId, out var context)) return;
        if (!context.NodeContexts.TryGetValue(nodeId, out var nodeContext)) return;

        Interlocked.Add(ref nodeContext.RecordsOut, count);

        if (samples != null && nodeContext.SampleOutputs.Count < 5)
        {
            foreach (var sample in samples.Take(5 - nodeContext.SampleOutputs.Count))
            {
                nodeContext.SampleOutputs.Add(sample);
            }
        }
    }

    /// <summary>
    /// Record a failed record.
    /// </summary>
    public void RecordFailure(string executionId, string nodeId, string? error = null)
    {
        if (!_activeExecutions.TryGetValue(executionId, out var context)) return;
        if (!context.NodeContexts.TryGetValue(nodeId, out var nodeContext)) return;

        Interlocked.Increment(ref nodeContext.RecordsFailed);

        if (error != null && nodeContext.Error == null)
        {
            nodeContext.Error = error;
        }
    }

    /// <summary>
    /// Mark a node as completed.
    /// </summary>
    public void CompleteNode(string executionId, string nodeId, string? error = null)
    {
        if (!_activeExecutions.TryGetValue(executionId, out var context)) return;
        if (!context.NodeContexts.TryGetValue(nodeId, out var nodeContext)) return;

        nodeContext.CompletedAt = DateTimeOffset.UtcNow;
        nodeContext.Status = string.IsNullOrEmpty(error) && nodeContext.RecordsFailed == 0
            ? ExecutionStatus.Completed
            : ExecutionStatus.Failed;

        if (error != null)
        {
            nodeContext.Error = error;
        }
    }

    /// <summary>
    /// Complete the execution successfully.
    /// </summary>
    public async Task CompleteExecutionAsync(string executionId, CancellationToken cancellationToken = default)
    {
        if (!_activeExecutions.TryRemove(executionId, out var context)) return;

        var nodes = BuildNodeRecords(context);
        var totalProcessed = nodes.Sum(n => n.RecordsIn);
        var totalFailed = nodes.Sum(n => n.RecordsFailed);

        await _store.CompleteAsync(executionId, totalProcessed, totalFailed, nodes, cancellationToken);
        _logger.LogInformation("Completed execution {ExecutionId}: {Processed} processed, {Failed} failed",
            executionId, totalProcessed, totalFailed);
    }

    /// <summary>
    /// Fail the execution.
    /// </summary>
    public async Task FailExecutionAsync(string executionId, Exception exception, CancellationToken cancellationToken = default)
    {
        _activeExecutions.TryRemove(executionId, out _);
        await _store.FailAsync(executionId, exception.Message, exception.StackTrace, cancellationToken);
        _logger.LogError(exception, "Execution {ExecutionId} failed", executionId);
    }

    /// <summary>
    /// Get current execution stats (for monitoring).
    /// </summary>
    public ExecutionStats? GetStats(string executionId)
    {
        if (!_activeExecutions.TryGetValue(executionId, out var context)) return null;

        return new ExecutionStats
        {
            ExecutionId = executionId,
            PipelineId = context.PipelineId,
            StartedAt = context.StartedAt,
            TotalRecordsIn = context.NodeContexts.Values.Sum(n => n.RecordsIn),
            TotalRecordsOut = context.NodeContexts.Values.Sum(n => n.RecordsOut),
            TotalRecordsFailed = context.NodeContexts.Values.Sum(n => n.RecordsFailed),
            NodeStats = context.NodeContexts.Values.Select(n => new NodeStats
            {
                NodeId = n.NodeId,
                RecordsIn = n.RecordsIn,
                RecordsOut = n.RecordsOut,
                RecordsFailed = n.RecordsFailed
            }).ToList()
        };
    }

    private static List<NodeExecutionRecord> BuildNodeRecords(ExecutionContext context)
    {
        return context.NodeContexts.Values.Select(n => new NodeExecutionRecord
        {
            NodeId = n.NodeId,
            NodeLabel = n.NodeLabel,
            ConnectorType = n.ConnectorType,
            Status = n.Status,
            RecordsIn = n.RecordsIn,
            RecordsOut = n.RecordsOut,
            RecordsFailed = n.RecordsFailed,
            DurationMs = n.CompletedAt.HasValue
                ? (long)(n.CompletedAt.Value - n.StartedAt).TotalMilliseconds
                : (long)(DateTimeOffset.UtcNow - n.StartedAt).TotalMilliseconds,
            Error = n.Error,
            SampleInputs = n.SampleInputs.Count > 0 ? n.SampleInputs : null,
            SampleOutputs = n.SampleOutputs.Count > 0 ? n.SampleOutputs : null
        }).ToList();
    }

    private sealed class ExecutionContext
    {
        public required string ExecutionId { get; init; }
        public required string PipelineId { get; init; }
        public DateTimeOffset StartedAt { get; init; }
        public ConcurrentDictionary<string, NodeExecutionContext> NodeContexts { get; } = new();
    }

    private sealed class NodeExecutionContext
    {
        public required string NodeId { get; init; }
        public string? NodeLabel { get; init; }
        public required string ConnectorType { get; init; }
        public DateTimeOffset StartedAt { get; init; }
        public DateTimeOffset? CompletedAt { get; set; }
        public ExecutionStatus Status { get; set; } = ExecutionStatus.Running;
        public long RecordsIn;
        public long RecordsOut;
        public long RecordsFailed;
        public string? Error { get; set; }
        public List<SampleRecord> SampleInputs { get; } = [];
        public List<SampleRecord> SampleOutputs { get; } = [];
    }
}

/// <summary>
/// Real-time execution stats.
/// </summary>
public record ExecutionStats
{
    public required string ExecutionId { get; init; }
    public required string PipelineId { get; init; }
    public DateTimeOffset StartedAt { get; init; }
    public long TotalRecordsIn { get; init; }
    public long TotalRecordsOut { get; init; }
    public long TotalRecordsFailed { get; init; }
    public required List<NodeStats> NodeStats { get; init; }
}

/// <summary>
/// Real-time node stats.
/// </summary>
public record NodeStats
{
    public required string NodeId { get; init; }
    public long RecordsIn { get; init; }
    public long RecordsOut { get; init; }
    public long RecordsFailed { get; init; }
}

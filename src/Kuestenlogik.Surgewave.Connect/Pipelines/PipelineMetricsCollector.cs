namespace Kuestenlogik.Surgewave.Connect.Pipelines;

using System.Collections.Concurrent;

/// <summary>
/// Collects real-time metrics per pipeline and node.
/// Thread-safe using Interlocked operations and ConcurrentDictionary.
/// </summary>
public sealed class PipelineMetricsCollector
{
    private readonly ConcurrentDictionary<string, PipelineMetricsState> _pipelines = new();

    public void RecordProcessed(string pipelineId, string nodeId, double latencyMs)
    {
        var state = _pipelines.GetOrAdd(pipelineId, _ => new PipelineMetricsState());
        var nodeState = state.Nodes.GetOrAdd(nodeId, _ => new NodeMetricsState());

        Interlocked.Increment(ref nodeState.RecordsProcessed);
        nodeState.Latency.Record(latencyMs);
    }

    public void RecordError(string pipelineId, string nodeId)
    {
        var state = _pipelines.GetOrAdd(pipelineId, _ => new PipelineMetricsState());
        var nodeState = state.Nodes.GetOrAdd(nodeId, _ => new NodeMetricsState());

        Interlocked.Increment(ref nodeState.ErrorCount);
    }

    public void RecordRetryAttempt(string pipelineId, string nodeId)
    {
        var state = _pipelines.GetOrAdd(pipelineId, _ => new PipelineMetricsState());
        var nodeState = state.Nodes.GetOrAdd(nodeId, _ => new NodeMetricsState());

        Interlocked.Increment(ref nodeState.RetryAttempts);
    }

    public void RecordRetryExhausted(string pipelineId, string nodeId)
    {
        var state = _pipelines.GetOrAdd(pipelineId, _ => new PipelineMetricsState());
        var nodeState = state.Nodes.GetOrAdd(nodeId, _ => new NodeMetricsState());

        Interlocked.Increment(ref nodeState.RetryExhausted);
    }

    public PipelineMetrics? GetMetrics(string pipelineId)
    {
        if (!_pipelines.TryGetValue(pipelineId, out var state))
            return null;

        var elapsed = (DateTimeOffset.UtcNow - state.StartedAt).TotalSeconds;
        var nodes = new Dictionary<string, NodeMetrics>();
        long totalRecords = 0;
        long totalErrors = 0;

        foreach (var (nodeId, nodeState) in state.Nodes)
        {
            var records = Interlocked.Read(ref nodeState.RecordsProcessed);
            var errors = Interlocked.Read(ref nodeState.ErrorCount);
            totalRecords += records;
            totalErrors += errors;

            nodes[nodeId] = new NodeMetrics
            {
                NodeId = nodeId,
                RecordsIn = records,
                RecordsOut = records - errors,
                Errors = errors,
                AvgLatencyMs = Math.Round(nodeState.Latency.Average, 2),
                P50LatencyMs = Math.Round(nodeState.Latency.GetPercentile(50), 2),
                P95LatencyMs = Math.Round(nodeState.Latency.GetPercentile(95), 2),
                P99LatencyMs = Math.Round(nodeState.Latency.GetPercentile(99), 2)
            };
        }

        return new PipelineMetrics
        {
            PipelineId = pipelineId,
            StartedAt = state.StartedAt,
            TotalRecordsProcessed = totalRecords,
            TotalErrors = totalErrors,
            RecordsPerSecond = elapsed > 0 ? Math.Round(totalRecords / elapsed, 2) : 0,
            Nodes = nodes
        };
    }

    public NodeMetrics? GetNodeMetrics(string pipelineId, string nodeId)
    {
        if (!_pipelines.TryGetValue(pipelineId, out var state))
            return null;

        if (!state.Nodes.TryGetValue(nodeId, out var nodeState))
            return null;

        var records = Interlocked.Read(ref nodeState.RecordsProcessed);
        var errors = Interlocked.Read(ref nodeState.ErrorCount);

        return new NodeMetrics
        {
            NodeId = nodeId,
            RecordsIn = records,
            RecordsOut = records - errors,
            Errors = errors,
            AvgLatencyMs = Math.Round(nodeState.Latency.Average, 2),
            P50LatencyMs = Math.Round(nodeState.Latency.GetPercentile(50), 2),
            P95LatencyMs = Math.Round(nodeState.Latency.GetPercentile(95), 2),
            P99LatencyMs = Math.Round(nodeState.Latency.GetPercentile(99), 2)
        };
    }

    public void Reset(string pipelineId)
    {
        _pipelines.TryRemove(pipelineId, out _);
    }
}

internal sealed class PipelineMetricsState
{
    public DateTimeOffset StartedAt { get; } = DateTimeOffset.UtcNow;
    public ConcurrentDictionary<string, NodeMetricsState> Nodes { get; } = new();
}

internal sealed class NodeMetricsState
{
    public long RecordsProcessed;
    public long ErrorCount;
    public long RetryAttempts;
    public long RetryExhausted;
    public LatencyHistogram Latency { get; } = new();
}

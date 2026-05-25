using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Streams.Monitoring;

/// <summary>
/// Per-processor-node metrics with OTEL instrumentation.
/// </summary>
public sealed class ProcessorNodeMetrics
{
    private readonly Counter<long> _recordsIn;
    private readonly Counter<long> _recordsOut;
    private readonly Histogram<double> _processLatency;
    private readonly Counter<long> _errors;
    private readonly Counter<long> _skipped;
    private readonly KeyValuePair<string, object?> _nodeTag;

    private long _totalIn;
    private long _totalOut;
    private long _totalErrors;
    private long _totalSkipped;

    public string NodeName { get; }
    public long TotalIn => _totalIn;
    public long TotalOut => _totalOut;
    public long TotalErrors => _totalErrors;

    /// <summary>Gets the total number of records skipped (filtered/dropped) by this node.</summary>
    public long TotalSkipped => _totalSkipped;

    public ProcessorNodeMetrics(Meter meter, string nodeName)
    {
        NodeName = nodeName;
        _nodeTag = new KeyValuePair<string, object?>("node.name", nodeName);

        _recordsIn = meter.CreateCounter<long>(
            "surgewave_streams_node_records_in_total",
            description: "Records received by processor node");

        _recordsOut = meter.CreateCounter<long>(
            "surgewave_streams_node_records_out_total",
            description: "Records emitted by processor node");

        _processLatency = meter.CreateHistogram<double>(
            "surgewave_streams_node_process_latency_ms",
            unit: "ms",
            description: "Processing latency per processor node");

        _errors = meter.CreateCounter<long>(
            "surgewave_streams_node_errors_total",
            description: "Errors per processor node");

        _skipped = meter.CreateCounter<long>(
            "surgewave_streams_node_records_skipped_total",
            description: "Records filtered or dropped by processor node");
    }

    public void RecordIn(int count = 1)
    {
        Interlocked.Add(ref _totalIn, count);
        _recordsIn.Add(count, _nodeTag);
    }

    public void RecordOut(int count = 1)
    {
        Interlocked.Add(ref _totalOut, count);
        _recordsOut.Add(count, _nodeTag);
    }

    public void RecordLatency(double ms)
    {
        _processLatency.Record(ms, _nodeTag);
    }

    public void RecordError()
    {
        Interlocked.Increment(ref _totalErrors);
        _errors.Add(1, _nodeTag);
    }

    /// <summary>Records a record that was filtered or dropped by this processor node.</summary>
    public void RecordSkipped(int count = 1)
    {
        Interlocked.Add(ref _totalSkipped, count);
        _skipped.Add(count, _nodeTag);
    }
}

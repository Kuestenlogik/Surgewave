namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Priority queue node that reorders records based on a priority field.
/// Buffers records and flushes them in priority order at configurable intervals.
/// Lower priority values are emitted first (min-heap semantics).
/// </summary>
[ConnectorMetadata(
    Name = "Priority",
    Description = "Reorders records by priority field using a buffered priority queue",
    Tags = "logic,priority,queue,reorder,sort")]
public sealed class PriorityNode : ProcessorConnector
{
    public override Type TaskClass => typeof(PriorityNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for priority-ordered records")
        .Define("priority.field", ConfigType.String, "$.priority", Importance.High,
            "JSONPath to the numeric priority field (lower = higher priority)")
        .Define("priority.default", ConfigType.Int, "100", Importance.Medium,
            "Default priority when field is missing or not numeric")
        .Define("priority.order", ConfigType.String, "asc", Importance.Medium,
            "Sort order: 'asc' (lower first) or 'desc' (higher first)")
        .Define("flush.interval.ms", ConfigType.Long, "1000", Importance.Medium,
            "Interval in milliseconds between priority flushes")
        .Define("flush.batch.size", ConfigType.Int, "0", Importance.Medium,
            "Max records to emit per flush (0 = flush all)")
        .Define("flush.on.put", ConfigType.Boolean, "false", Importance.Low,
            "Also flush immediately on each PutAsync call");
}

#pragma warning disable CA2213 // Timer is disposed in Stop()
internal sealed class PriorityNodeTask : ProcessorTask
{
    private string _priorityField = "$.priority";
    private int _defaultPriority = 100;
    private string _order = "asc";
    private long _flushIntervalMs = 1000;
    private int _flushBatchSize;
    private bool _flushOnPut;

    private readonly object _lock = new();
    private readonly List<PriorityEntry> _buffer = [];
    private Timer? _flushTimer;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _priorityField = config.TryGetValue("priority.field", out var pf) ? pf : "$.priority";
        if (config.TryGetValue("priority.default", out var pd) && int.TryParse(pd, out var defaultPri))
            _defaultPriority = defaultPri;
        _order = config.TryGetValue("priority.order", out var o) ? o.ToLowerInvariant() : "asc";
        if (config.TryGetValue("flush.interval.ms", out var fi) && long.TryParse(fi, out var flushMs))
            _flushIntervalMs = flushMs;
        if (config.TryGetValue("flush.batch.size", out var fb) && int.TryParse(fb, out var batchSize))
            _flushBatchSize = batchSize;
        if (config.TryGetValue("flush.on.put", out var fp) && bool.TryParse(fp, out var flushOnPut))
            _flushOnPut = flushOnPut;

        _flushTimer = new Timer(OnFlushTimer, null, _flushIntervalMs, _flushIntervalMs);
    }

    public override void Stop()
    {
        _flushTimer?.Dispose();
        _flushTimer = null;

        // Flush remaining records
        FlushBuffer(int.MaxValue);

        lock (_lock)
        {
            _buffer.Clear();
        }

        base.Stop();
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            foreach (var record in records)
            {
                var priority = ExtractPriority(record);
                _buffer.Add(new PriorityEntry(record, priority));
            }
        }

        if (_flushOnPut)
        {
            var limit = _flushBatchSize > 0 ? _flushBatchSize : int.MaxValue;
            FlushBuffer(limit);
        }

        return Task.CompletedTask;
    }

    internal void OnFlushTimer(object? state)
    {
        var limit = _flushBatchSize > 0 ? _flushBatchSize : int.MaxValue;
        FlushBuffer(limit);
    }

    private void FlushBuffer(int maxRecords)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return;

        List<PriorityEntry> toEmit;

        lock (_lock)
        {
            if (_buffer.Count == 0)
                return;

            // Sort: asc = lower priority first, desc = higher priority first
            if (_order == "desc")
                _buffer.Sort((a, b) => b.Priority.CompareTo(a.Priority));
            else
                _buffer.Sort((a, b) => a.Priority.CompareTo(b.Priority));

            var count = Math.Min(maxRecords, _buffer.Count);
            toEmit = _buffer.GetRange(0, count);
            _buffer.RemoveRange(0, count);
        }

        foreach (var entry in toEmit)
        {
            var key = GetKeyString(entry.Record);
            var headers = ConvertHeaders(entry.Record.Headers) ?? [];
            headers["_priority"] = entry.Priority.ToString(CultureInfo.InvariantCulture);
            EmitRecord(key, entry.Record.Value, headers);
        }
    }

    private int ExtractPriority(SinkRecord record)
    {
        if (record.Value is null || record.Value.Length == 0)
            return _defaultPriority;

        try
        {
            using var doc = JsonDocument.Parse(record.Value);
            var element = ConditionEvaluator.GetJsonPath(doc.RootElement, _priorityField);

            if (element.ValueKind == JsonValueKind.Number)
                return element.GetInt32();

            if (element.ValueKind == JsonValueKind.String &&
                int.TryParse(element.GetString(), out var parsed))
                return parsed;

            return _defaultPriority;
        }
        catch
        {
            return _defaultPriority;
        }
    }

    /// <summary>
    /// Expose buffer count for testing.
    /// </summary>
    internal int BufferCount
    {
        get { lock (_lock) { return _buffer.Count; } }
    }
}

internal sealed class PriorityEntry(SinkRecord record, int priority)
{
    public SinkRecord Record { get; } = record;
    public int Priority { get; } = priority;
}

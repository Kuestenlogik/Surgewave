namespace Kuestenlogik.Surgewave.Connect.Nodes.Workflow;

using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Collects records into batches by count or time window, then emits them as a batch.
/// Supports grouping by a field value for separate accumulators per group.
/// </summary>
[ConnectorMetadata(
    Name = "Accumulator",
    Description = "Collect records into batches by count or time window",
    Tags = "workflow,batch,accumulator,aggregate,window",
    Icon = "Inventory2")]
public sealed class AccumulatorNode : ProcessorConnector
{
    public override Type TaskClass => typeof(AccumulatorNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("batch.size", ConfigType.Int, "10", Importance.High,
            "Records per batch before emitting")
        .Define("window.ms", ConfigType.Long, "5000", Importance.High,
            "Time window in ms (emit even if batch not full)")
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for accumulated batches")
        .Define("output.format", ConfigType.String, "array", Importance.Medium,
            "Output format: 'array' (JSON array) or 'individual' (separate records with batch headers)")
        .Define("group.by.field", ConfigType.String, "", Importance.Low,
            "Group into separate batches by field value");
}

#pragma warning disable CA2213 // Timer disposed in Stop()
internal sealed class AccumulatorNodeTask : ProcessorTask
{
    private int _batchSize = 10;
    private long _windowMs = 5000;
    private string _outputFormat = "array";
    private string _groupByField = "";

    private readonly ConcurrentDictionary<string, AccumulatorGroup> _groups = new();
    private Timer? _windowTimer;
    private readonly object _flushLock = new();

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        if (config.TryGetValue("batch.size", out var bs) && int.TryParse(bs, out var batchSize))
            _batchSize = batchSize;
        if (config.TryGetValue("window.ms", out var wm) && long.TryParse(wm, out var windowMs))
            _windowMs = windowMs;
        if (config.TryGetValue("output.format", out var of))
            _outputFormat = of;
        if (config.TryGetValue("group.by.field", out var gbf))
            _groupByField = gbf;

        _windowTimer = new Timer(OnWindowElapsed, null, _windowMs, _windowMs);
    }

    public override void Stop()
    {
        _windowTimer?.Dispose();
        _windowTimer = null;

        // Flush remaining records
        lock (_flushLock)
        {
            foreach (var group in _groups.Values)
            {
                FlushGroup(group);
            }

            _groups.Clear();
        }

        base.Stop();
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var groupKey = ExtractGroupKey(record);
            var group = _groups.GetOrAdd(groupKey, _ => new AccumulatorGroup());

            lock (group.Lock)
            {
                group.Records.Add(record);

                if (group.Records.Count >= _batchSize)
                {
                    FlushGroup(group);
                }
            }
        }

        return Task.CompletedTask;
    }

    internal void OnWindowElapsed(object? state)
    {
        lock (_flushLock)
        {
            foreach (var group in _groups.Values)
            {
                lock (group.Lock)
                {
                    if (group.Records.Count > 0)
                    {
                        FlushGroup(group);
                    }
                }
            }
        }
    }

    private void FlushGroup(AccumulatorGroup group)
    {
        if (group.Records.Count == 0)
            return;

        var batch = new List<SinkRecord>(group.Records);
        group.Records.Clear();

        if (_outputFormat == "individual")
        {
            EmitIndividual(batch);
        }
        else
        {
            EmitArray(batch);
        }
    }

    private void EmitArray(List<SinkRecord> batch)
    {
        var array = new JsonArray();
        foreach (var record in batch)
        {
            try
            {
                var node = JsonNode.Parse(record.Value);
                array.Add(node);
            }
            catch (JsonException)
            {
                array.Add(Encoding.UTF8.GetString(record.Value));
            }
        }

        var headers = new Dictionary<string, string>
        {
            ["x-batch-size"] = batch.Count.ToString(CultureInfo.InvariantCulture),
            ["x-batch-timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)
        };

        EmitRecord(null, array.ToJsonString(), headers);
    }

    private void EmitIndividual(List<SinkRecord> batch)
    {
        var batchId = Guid.NewGuid().ToString("N");

        for (var i = 0; i < batch.Count; i++)
        {
            var record = batch[i];
            var headers = ConvertHeaders(record.Headers) ?? [];
            headers["x-batch-id"] = batchId;
            headers["x-batch-index"] = i.ToString(CultureInfo.InvariantCulture);
            headers["x-batch-size"] = batch.Count.ToString(CultureInfo.InvariantCulture);

            EmitRecord(GetKeyString(record), record.Value, headers);
        }
    }

    private string ExtractGroupKey(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_groupByField))
            return "__default__";

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return "__default__";

        var parts = _groupByField.TrimStart('$', '.').Split('.');
        var current = doc.RootElement;

        foreach (var part in parts)
        {
            if (string.IsNullOrEmpty(part))
                continue;

            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(part, out var next))
                return "__default__";

            current = next;
        }

        return current.ValueKind switch
        {
            JsonValueKind.String => current.GetString() ?? "__default__",
            JsonValueKind.Number => current.GetDouble().ToString(CultureInfo.InvariantCulture),
            _ => current.GetRawText()
        };
    }

    /// <summary>
    /// Exposes total accumulated records count for testing.
    /// </summary>
    internal int TotalAccumulatedCount
    {
        get
        {
            var count = 0;
            foreach (var group in _groups.Values)
            {
                lock (group.Lock)
                {
                    count += group.Records.Count;
                }
            }
            return count;
        }
    }
}

internal sealed class AccumulatorGroup
{
    public List<SinkRecord> Records { get; } = [];
    public object Lock { get; } = new();
}

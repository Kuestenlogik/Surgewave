namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

/// <summary>
/// Aggregate node that collects and aggregates records over a time window or count.
/// </summary>
[ConnectorMetadata(
    Name = "Aggregate",
    Description = "Aggregate records with count/sum/avg",
    Tags = "transform,aggregate,group")]
public sealed class AggregateNode : ProcessorConnector
{
    public override Type TaskClass => typeof(AggregateNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for aggregated records")
        .Define("group.by", ConfigType.String, "", Importance.High,
            "JSONPath to group by field")
        .Define("window.size.ms", ConfigType.Long, "60000", Importance.Medium,
            "Time window in milliseconds")
        .Define("window.count", ConfigType.Int, "0", Importance.Medium,
            "Emit after N records per group");
}

internal sealed class AggregateNodeTask : ProcessorTask, IDisposable
{
    private string _groupBy = "";
    private long _windowSizeMs = 60000;
    private int _windowCount;
    private readonly List<AggregationDef> _aggregations = [];
    private readonly ConcurrentDictionary<string, AggregationState> _states = new();
    private Timer? _windowTimer;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _groupBy = config.TryGetValue("group.by", out var g) ? g : "";
        _windowSizeMs = config.TryGetValue("window.size.ms", out var w) && long.TryParse(w, out var ws) ? ws : 60000;
        _windowCount = config.TryGetValue("window.count", out var c) && int.TryParse(c, out var wc) ? wc : 0;

        foreach (var (key, value) in config)
        {
            if (key.StartsWith("aggregate.", StringComparison.OrdinalIgnoreCase))
            {
                var name = key[10..];
                var parts = value.Split(':');
                var type = parts[0].ToLowerInvariant();
                var path = parts.Length > 1 ? parts[1] : "";

                _aggregations.Add(new AggregationDef(name, type, path));
            }
        }

        if (_windowSizeMs > 0)
        {
            _windowTimer = new Timer(OnWindowExpired, null, _windowSizeMs, _windowSizeMs);
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var groupKey = GetGroupKey(record);
            var state = _states.GetOrAdd(groupKey, _ => new AggregationState());

            lock (state)
            {
                UpdateState(state, record);
                state.Count++;

                if (_windowCount > 0 && state.Count >= _windowCount)
                {
                    EmitAggregation(groupKey, state);
                    state.Reset();
                }
            }
        }

        return Task.CompletedTask;
    }

    private void OnWindowExpired(object? state)
    {
        foreach (var (groupKey, aggState) in _states)
        {
            lock (aggState)
            {
                if (aggState.Count > 0)
                {
                    EmitAggregation(groupKey, aggState);
                    aggState.Reset();
                }
            }
        }
    }

    private string GetGroupKey(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_groupBy))
            return "_all_";

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return "_null_";

        var element = ConditionEvaluator.GetJsonPath(doc.RootElement, _groupBy);
        if (element.ValueKind == JsonValueKind.Undefined)
            return "_null_";

        return element.ToString();
    }

    private void UpdateState(AggregationState state, SinkRecord record)
    {
        using var doc = ParseJsonValue(record);
        if (doc is null)
            return;

        foreach (var agg in _aggregations)
        {
            if (agg.Type == "count")
                continue;

            var element = ConditionEvaluator.GetJsonPath(doc.RootElement, agg.Path);
            if (element.ValueKind == JsonValueKind.Number)
            {
                var value = element.GetDouble();
                state.UpdateAggregation(agg.Name, agg.Type, value);
            }
        }
    }

    private void EmitAggregation(string groupKey, AggregationState state)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return;

        var result = new JsonObject
        {
            ["_group"] = groupKey,
            ["_count"] = state.Count,
            ["_window_start"] = state.WindowStart.ToString("O"),
            ["_window_end"] = DateTimeOffset.UtcNow.ToString("O")
        };

        foreach (var agg in _aggregations)
        {
            if (agg.Type == "count")
            {
                result[agg.Name] = state.Count;
            }
            else if (state.Aggregations.TryGetValue(agg.Name, out var aggValue))
            {
                result[agg.Name] = agg.Type switch
                {
                    "avg" => aggValue.Count > 0 ? aggValue.Sum / aggValue.Count : 0,
                    "sum" => aggValue.Sum,
                    "min" => aggValue.Min,
                    "max" => aggValue.Max,
                    _ => 0
                };
            }
        }

        EmitRecord(groupKey, result.ToJsonString());
    }

    public new void Dispose()
    {
        _windowTimer?.Dispose();
        base.Dispose();
    }
}

internal sealed record AggregationDef(string Name, string Type, string Path);

internal sealed class AggregationState
{
    public int Count { get; set; }
    public DateTimeOffset WindowStart { get; private set; } = DateTimeOffset.UtcNow;
    public Dictionary<string, AggregationValue> Aggregations { get; } = new();

    public void UpdateAggregation(string name, string type, double value)
    {
        if (!Aggregations.TryGetValue(name, out var agg))
        {
            agg = new AggregationValue();
            Aggregations[name] = agg;
        }

        agg.Count++;
        agg.Sum += value;
        agg.Min = Math.Min(agg.Min, value);
        agg.Max = Math.Max(agg.Max, value);
    }

    public void Reset()
    {
        Count = 0;
        WindowStart = DateTimeOffset.UtcNow;
        Aggregations.Clear();
    }
}

internal sealed class AggregationValue
{
    public int Count { get; set; }
    public double Sum { get; set; }
    public double Min { get; set; } = double.MaxValue;
    public double Max { get; set; } = double.MinValue;
}

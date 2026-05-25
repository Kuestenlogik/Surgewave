namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Join node that correlates records from two input topics by key.
/// Supports inner join and left join with a configurable time window.
/// </summary>
[ConnectorMetadata(
    Name = "Join",
    Description = "Correlates records from two input topics by key (inner/left join)",
    Tags = "logic,join,lookup,enrich")]
public sealed class JoinNode : ProcessorConnector
{
    public override Type TaskClass => typeof(JoinNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for joined records")
        .Define("join.type", ConfigType.String, "inner", Importance.High,
            "Join type: 'inner' or 'left'")
        .Define("join.window.ms", ConfigType.Long, "60000", Importance.Medium,
            "Time window in milliseconds for matching records")
        .Define("join.key.left", ConfigType.String, "", Importance.High,
            "JSONPath expression for the left join key (e.g., $.userId)")
        .Define("join.key.right", ConfigType.String, "", Importance.High,
            "JSONPath expression for the right join key (e.g., $.id)")
        .Define("left.topic", ConfigType.String, "", Importance.High,
            "Internal topic name for left-side records")
        .Define("right.topic", ConfigType.String, "", Importance.High,
            "Internal topic name for right-side records")
        .Define("output.left.prefix", ConfigType.String, "", Importance.Low,
            "Prefix for left-side fields in output (e.g., 'order.')")
        .Define("output.right.prefix", ConfigType.String, "", Importance.Low,
            "Prefix for right-side fields in output (e.g., 'user.')");
}

#pragma warning disable CA2213 // Timer is disposed in Stop()
internal sealed class JoinNodeTask : ProcessorTask
{
    private string _joinType = "inner";
    private long _windowMs = 60000;
    private string _leftKeyPath = "";
    private string _rightKeyPath = "";
    private string _leftTopic = "";
    private string _rightTopic = "";
    private string _leftPrefix = "";
    private string _rightPrefix = "";

    private readonly ConcurrentDictionary<string, JoinState> _state = new();
    private Timer? _expirationTimer;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _joinType = config.TryGetValue("join.type", out var jt) ? jt.ToLowerInvariant() : "inner";
        if (config.TryGetValue("join.window.ms", out var wm) && long.TryParse(wm, out var windowMs))
            _windowMs = windowMs;
        _leftKeyPath = config.TryGetValue("join.key.left", out var lk) ? lk : "";
        _rightKeyPath = config.TryGetValue("join.key.right", out var rk) ? rk : "";
        _leftTopic = config.TryGetValue("left.topic", out var lt) ? lt : "";
        _rightTopic = config.TryGetValue("right.topic", out var rt) ? rt : "";
        _leftPrefix = config.TryGetValue("output.left.prefix", out var lp) ? lp : "";
        _rightPrefix = config.TryGetValue("output.right.prefix", out var rp) ? rp : "";

        // Start expiration timer
        _expirationTimer = new Timer(OnWindowExpired, null, _windowMs, _windowMs);
    }

    public override void Stop()
    {
        _expirationTimer?.Dispose();
        _expirationTimer = null;
        _state.Clear();
        base.Stop();
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var isLeft = IsLeftRecord(record);
            var keyPath = isLeft ? _leftKeyPath : _rightKeyPath;
            var joinKey = ExtractKey(record, keyPath);

            if (joinKey is null)
                continue;

            var state = _state.GetOrAdd(joinKey, _ => new JoinState());
            var buffered = new BufferedRecord(record, DateTimeOffset.UtcNow);

            if (isLeft)
            {
                state.LeftRecords.Add(buffered);

                // Try to match with existing right records
                foreach (var right in state.RightRecords)
                {
                    if (!right.Matched)
                    {
                        right.Matched = true;
                        buffered.Matched = true;
                        EmitJoinedRecord(record, right.Record, joinKey);
                    }
                }
            }
            else
            {
                state.RightRecords.Add(buffered);

                // Try to match with existing left records
                foreach (var left in state.LeftRecords)
                {
                    if (!left.Matched)
                    {
                        left.Matched = true;
                        buffered.Matched = true;
                        EmitJoinedRecord(left.Record, record, joinKey);
                    }
                }
            }
        }

        return Task.CompletedTask;
    }

    private bool IsLeftRecord(SinkRecord record)
    {
        return record.Topic == _leftTopic;
    }

    private static string? ExtractKey(SinkRecord record, string keyPath)
    {
        if (string.IsNullOrEmpty(keyPath))
        {
            // Use record key as join key
            return record.Key != null ? Encoding.UTF8.GetString(record.Key) : null;
        }

        // Extract from JSON value using JSONPath
        if (record.Value is null || record.Value.Length == 0)
            return null;

        try
        {
            using var doc = JsonDocument.Parse(record.Value);
            var element = ConditionEvaluator.GetJsonPath(doc.RootElement, keyPath);
            if (element.ValueKind == JsonValueKind.Undefined)
                return null;

            return element.ValueKind switch
            {
                JsonValueKind.String => element.GetString(),
                JsonValueKind.Number => element.GetRawText(),
                _ => element.GetRawText()
            };
        }
        catch
        {
            return null;
        }
    }

    private void EmitJoinedRecord(SinkRecord left, SinkRecord? right, string joinKey)
    {
        var output = new JsonObject();

        // Merge left-side fields
        MergeJsonInto(output, left.Value, _leftPrefix);

        // Merge right-side fields
        if (right?.Value != null)
        {
            MergeJsonInto(output, right.Value, _rightPrefix);
        }

        // Add join metadata
        output["_join_key"] = joinKey;
        output["_join_timestamp"] = DateTimeOffset.UtcNow.ToString("o");

        if (right == null)
        {
            output["_join_type"] = "left_unmatched";
        }

        var key = GetKeyString(left);
        EmitRecord(key, output.ToJsonString());
    }

    private static void MergeJsonInto(JsonObject target, byte[]? value, string prefix)
    {
        if (value is null || value.Length == 0)
            return;

        try
        {
            using var doc = JsonDocument.Parse(value);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return;

            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                var fieldName = string.IsNullOrEmpty(prefix) ? prop.Name : $"{prefix}{prop.Name}";
                target[fieldName] = JsonNode.Parse(prop.Value.GetRawText());
            }
        }
        catch
        {
            // Non-JSON values are skipped
        }
    }

    private void OnWindowExpired(object? state)
    {
        var cutoff = DateTimeOffset.UtcNow.AddMilliseconds(-_windowMs);
        var keysToRemove = new List<string>();

        foreach (var (key, joinState) in _state)
        {
            // Emit unmatched left records for left join
            if (_joinType == "left")
            {
                foreach (var left in joinState.LeftRecords)
                {
                    if (!left.Matched && left.ArrivedAt < cutoff)
                    {
                        left.Matched = true;
                        EmitJoinedRecord(left.Record, null, key);
                    }
                }
            }

            // Clean up expired records
            joinState.LeftRecords.RemoveAll(r => r.ArrivedAt < cutoff);
            joinState.RightRecords.RemoveAll(r => r.ArrivedAt < cutoff);

            if (joinState.LeftRecords.Count == 0 && joinState.RightRecords.Count == 0)
            {
                keysToRemove.Add(key);
            }
        }

        foreach (var key in keysToRemove)
        {
            _state.TryRemove(key, out _);
        }
    }
}

internal sealed class JoinState
{
    public List<BufferedRecord> LeftRecords { get; } = [];
    public List<BufferedRecord> RightRecords { get; } = [];
}

internal sealed class BufferedRecord(SinkRecord record, DateTimeOffset arrivedAt)
{
    public SinkRecord Record { get; } = record;
    public DateTimeOffset ArrivedAt { get; } = arrivedAt;
    public bool Matched { get; set; }
}

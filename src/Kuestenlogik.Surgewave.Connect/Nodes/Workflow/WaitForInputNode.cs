namespace Kuestenlogik.Surgewave.Connect.Nodes.Workflow;

using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Pauses workflow execution and waits for an external signal/message before continuing.
/// Stores waiting records by correlation key and merges them with incoming signals.
/// </summary>
[ConnectorMetadata(
    Name = "Wait for Input",
    Description = "Pause workflow and wait for external input (user message, API call, or signal topic)",
    Tags = "workflow,wait,input,human,interaction",
    Icon = "PauseCircle")]
public sealed class WaitForInputNode : ProcessorConnector
{
    public override Type TaskClass => typeof(WaitForInputNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("signal.topic", ConfigType.String, "", Importance.High,
            "Topic to listen for input/signals")
        .Define("correlation.field", ConfigType.String, "", Importance.High,
            "Field to correlate input with waiting record (e.g., 'session_id')")
        .Define("timeout.ms", ConfigType.Long, "300000", Importance.Medium,
            "How long to wait before timeout (default 5 minutes)")
        .Define("timeout.action", ConfigType.String, "emit_timeout", Importance.Medium,
            "Action on timeout: emit_timeout, drop, emit_error")
        .Define("state.topic", ConfigType.String, "", Importance.Medium,
            "Internal compacted topic for persisting waiting state")
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic after input received")
        .Define("merge.strategy", ConfigType.String, "combine", Importance.Medium,
            "How to merge original + input: combine, replace, append");
}

#pragma warning disable CA2213 // Timer disposed in Stop()
internal sealed class WaitForInputNodeTask : ProcessorTask
{
    private string _signalTopic = "";
    private string _correlationField = "";
    private long _timeoutMs = 300000;
    private string _timeoutAction = "emit_timeout";
    private string _mergeStrategy = "combine";

    private readonly ConcurrentDictionary<string, WaitingEntry> _waitingRecords = new();
    private Timer? _timeoutTimer;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        if (config.TryGetValue("signal.topic", out var st))
            _signalTopic = st;
        if (config.TryGetValue("correlation.field", out var cf))
            _correlationField = cf;
        if (config.TryGetValue("timeout.ms", out var tm) && long.TryParse(tm, out var timeoutMs))
            _timeoutMs = timeoutMs;
        if (config.TryGetValue("timeout.action", out var ta))
            _timeoutAction = ta;
        if (config.TryGetValue("merge.strategy", out var ms))
            _mergeStrategy = ms;

        _timeoutTimer = new Timer(OnTimeoutCheck, null, 1000, 1000);
    }

    public override void Stop()
    {
        _timeoutTimer?.Dispose();
        _timeoutTimer = null;
        _waitingRecords.Clear();
        base.Stop();
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Topic == _signalTopic)
            {
                // This is a signal — try to match with a waiting record
                HandleSignal(record);
            }
            else
            {
                // This is a workflow record — store and wait for signal
                HandleWaitingRecord(record);
            }
        }

        return Task.CompletedTask;
    }

    private void HandleWaitingRecord(SinkRecord record)
    {
        var correlationKey = ExtractCorrelationKey(record);
        if (string.IsNullOrEmpty(correlationKey))
        {
            // No correlation key — emit error
            EmitError(record, new InvalidOperationException(
                $"Missing correlation field '{_correlationField}' in record"));
            return;
        }

        var entry = new WaitingEntry(record, DateTimeOffset.UtcNow);
        _waitingRecords[correlationKey] = entry;
    }

    private void HandleSignal(SinkRecord signal)
    {
        var correlationKey = ExtractCorrelationKey(signal);
        if (string.IsNullOrEmpty(correlationKey))
            return;

        if (!_waitingRecords.TryRemove(correlationKey, out var waitingEntry))
            return;

        // Merge original record with signal
        var merged = MergeRecords(waitingEntry.Record, signal);
        var key = GetKeyString(waitingEntry.Record);
        var headers = ConvertHeaders(waitingEntry.Record.Headers);
        EmitRecord(key, merged, headers);
    }

    private string? ExtractCorrelationKey(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_correlationField))
            return null;

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return null;

        var parts = _correlationField.TrimStart('$', '.').Split('.');
        var current = doc.RootElement;

        foreach (var part in parts)
        {
            if (string.IsNullOrEmpty(part))
                continue;

            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(part, out var next))
                return null;

            current = next;
        }

        return current.ValueKind switch
        {
            JsonValueKind.String => current.GetString(),
            JsonValueKind.Number => current.GetDouble().ToString(CultureInfo.InvariantCulture),
            _ => current.GetRawText()
        };
    }

    private string MergeRecords(SinkRecord original, SinkRecord signal)
    {
        return _mergeStrategy switch
        {
            "replace" => Encoding.UTF8.GetString(signal.Value),
            "append" => AppendRecords(original, signal),
            _ => CombineRecords(original, signal) // "combine" is the default
        };
    }

    private static string CombineRecords(SinkRecord original, SinkRecord signal)
    {
        try
        {
            var origObj = JsonNode.Parse(original.Value);
            var signalObj = JsonNode.Parse(signal.Value);

            if (origObj is JsonObject origJson && signalObj is JsonObject signalJson)
            {
                foreach (var prop in signalJson)
                {
                    origJson[prop.Key] = prop.Value?.DeepClone();
                }

                return origJson.ToJsonString();
            }
        }
        catch (JsonException)
        {
            // Fall through to concatenation
        }

        return Encoding.UTF8.GetString(original.Value) + Encoding.UTF8.GetString(signal.Value);
    }

    private static string AppendRecords(SinkRecord original, SinkRecord signal)
    {
        try
        {
            var origObj = JsonNode.Parse(original.Value);
            var signalObj = JsonNode.Parse(signal.Value);

            if (origObj is JsonObject origJson)
            {
                origJson["_input"] = signalObj?.DeepClone();
                return origJson.ToJsonString();
            }
        }
        catch (JsonException)
        {
            // Fall through
        }

        return Encoding.UTF8.GetString(original.Value) + Encoding.UTF8.GetString(signal.Value);
    }

    internal void OnTimeoutCheck(object? state)
    {
        var now = DateTimeOffset.UtcNow;
        var expiredKeys = new List<string>();

        foreach (var (key, entry) in _waitingRecords)
        {
            if ((now - entry.WaitingSince).TotalMilliseconds >= _timeoutMs)
            {
                expiredKeys.Add(key);
            }
        }

        foreach (var key in expiredKeys)
        {
            if (!_waitingRecords.TryRemove(key, out var entry))
                continue;

            switch (_timeoutAction)
            {
                case "emit_timeout":
                    var headers = ConvertHeaders(entry.Record.Headers) ?? [];
                    headers["x-wait-timeout"] = "true";
                    EmitRecord(GetKeyString(entry.Record), entry.Record.Value, headers);
                    break;

                case "emit_error":
                    EmitError(entry.Record, new TimeoutException(
                        $"Wait for input timed out after {_timeoutMs}ms"));
                    break;

                case "drop":
                default:
                    // Silently drop
                    break;
            }
        }
    }

    /// <summary>
    /// Exposes the waiting records count for testing.
    /// </summary>
    internal int WaitingCount => _waitingRecords.Count;
}

internal sealed class WaitingEntry(SinkRecord record, DateTimeOffset waitingSince)
{
    public SinkRecord Record { get; } = record;
    public DateTimeOffset WaitingSince { get; } = waitingSince;
}

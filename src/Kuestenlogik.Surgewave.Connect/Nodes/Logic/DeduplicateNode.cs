namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Filters duplicate records by key within a configurable time window.
/// Supports "first" (keep first occurrence) and "last" (keep last occurrence) strategies.
/// </summary>
[ConnectorMetadata(
    Name = "Deduplicate",
    Description = "Filters duplicate records by key within a time window",
    Tags = "logic,deduplicate,filter,unique")]
public sealed class DeduplicateNode : ProcessorConnector
{
    public override Type TaskClass => typeof(DeduplicateNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for deduplicated records")
        .Define("dedup.key", ConfigType.String, "", Importance.High,
            "JSONPath for dedup key (empty = use record key)")
        .Define("dedup.window.ms", ConfigType.Long, "300000", Importance.Medium,
            "Time window in milliseconds for duplicate detection (default 5 min)")
        .Define("dedup.strategy", ConfigType.String, "first", Importance.Medium,
            "Strategy: 'first' (keep first) or 'last' (keep last, emit on window expiry)");
}

#pragma warning disable CA2213 // Timer is disposed in Stop()
internal sealed class DeduplicateNodeTask : ProcessorTask
{
    private string _keyPath = "";
    private long _windowMs = 300000;
    private string _strategy = "first";

    private readonly ConcurrentDictionary<string, DeduplicationEntry> _state = new();
    private Timer? _expirationTimer;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _keyPath = config.TryGetValue("dedup.key", out var kp) ? kp : "";
        if (config.TryGetValue("dedup.window.ms", out var wm) && long.TryParse(wm, out var windowMs))
            _windowMs = windowMs;
        _strategy = config.TryGetValue("dedup.strategy", out var s) ? s.ToLowerInvariant() : "first";

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
            var key = ExtractKey(record);
            if (key is null)
                continue;

            if (_strategy == "last")
            {
                // Always update state, emit on window expiry
                _state[key] = new DeduplicationEntry(record, DateTimeOffset.UtcNow, false);
            }
            else
            {
                // "first" strategy: emit only if key not yet seen
                if (_state.TryAdd(key, new DeduplicationEntry(record, DateTimeOffset.UtcNow, true)))
                {
                    EmitDedupRecord(record);
                }
            }
        }

        return Task.CompletedTask;
    }

    private string? ExtractKey(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_keyPath))
        {
            return record.Key != null ? Encoding.UTF8.GetString(record.Key) : null;
        }

        if (record.Value is null || record.Value.Length == 0)
            return null;

        try
        {
            using var doc = JsonDocument.Parse(record.Value);
            var element = ConditionEvaluator.GetJsonPath(doc.RootElement, _keyPath);
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

    private void EmitDedupRecord(SinkRecord record)
    {
        var key = GetKeyString(record);
        var headers = ConvertHeaders(record.Headers);
        EmitRecord(key, record.Value, headers);
    }

    internal void OnWindowExpired(object? state)
    {
        var cutoff = DateTimeOffset.UtcNow.AddMilliseconds(-_windowMs);
        var keysToRemove = new List<string>();

        foreach (var (key, entry) in _state)
        {
            if (entry.ArrivedAt < cutoff)
            {
                // For "last" strategy, emit records that haven't been emitted yet
                if (_strategy == "last" && !entry.Emitted)
                {
                    EmitDedupRecord(entry.Record);
                }

                keysToRemove.Add(key);
            }
        }

        foreach (var key in keysToRemove)
        {
            _state.TryRemove(key, out _);
        }
    }
}

internal sealed class DeduplicationEntry(SinkRecord record, DateTimeOffset arrivedAt, bool emitted)
{
    public SinkRecord Record { get; } = record;
    public DateTimeOffset ArrivedAt { get; } = arrivedAt;
    public bool Emitted { get; } = emitted;
}

namespace Kuestenlogik.Surgewave.Connect.Nodes.Workflow;

using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Read/Write persistent key-value state within a workflow.
/// Maintains an in-memory cache backed by a compacted topic for persistence.
/// </summary>
[ConnectorMetadata(
    Name = "State Store",
    Description = "Read and write persistent key-value state for workflow memory",
    Tags = "workflow,state,memory,store,persistence",
    Icon = "Storage")]
public sealed class StateNode : ProcessorConnector
{
    public override Type TaskClass => typeof(StateNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("state.topic", ConfigType.String, "", Importance.High,
            "Compacted topic for state storage")
        .Define("operation", ConfigType.String, "read_write", Importance.High,
            "Operation mode: read, write, or read_write")
        .Define("key.field", ConfigType.String, "", Importance.High,
            "JSON field for state key")
        .Define("value.field", ConfigType.String, "", Importance.High,
            "JSON field for state value (write)")
        .Define("output.field", ConfigType.String, "state", Importance.Medium,
            "Field to put read state into")
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for processed records")
        .Define("ttl.ms", ConfigType.Long, "0", Importance.Low,
            "State entry TTL in ms (0 = no expiry)");
}

internal sealed class StateNodeTask : ProcessorTask
{
    private string _stateTopic = "";
    private string _operation = "read_write";
    private string _keyField = "";
    private string _valueField = "";
    private string _outputField = "state";
    private long _ttlMs;

    private readonly ConcurrentDictionary<string, StateEntry> _cache = new();

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        if (config.TryGetValue("state.topic", out var st))
            _stateTopic = st;
        if (config.TryGetValue("operation", out var op))
            _operation = op;
        if (config.TryGetValue("key.field", out var kf))
            _keyField = kf;
        if (config.TryGetValue("value.field", out var vf))
            _valueField = vf;
        if (config.TryGetValue("output.field", out var of))
            _outputField = of;
        if (config.TryGetValue("ttl.ms", out var ttl) && long.TryParse(ttl, out var ttlMs))
            _ttlMs = ttlMs;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            // Bootstrap from state topic — store without emitting
            if (record.Topic == _stateTopic)
            {
                BootstrapFromStateTopic(record);
                continue;
            }

            var stateKey = ExtractField(record, _keyField);
            if (string.IsNullOrEmpty(stateKey))
            {
                EmitError(record, new InvalidOperationException(
                    $"Missing key field '{_keyField}' in record"));
                continue;
            }

            switch (_operation)
            {
                case "read":
                    ProcessRead(record, stateKey);
                    break;
                case "write":
                    ProcessWrite(record, stateKey);
                    break;
                case "read_write":
                default:
                    ProcessReadWrite(record, stateKey);
                    break;
            }
        }

        return Task.CompletedTask;
    }

    private void ProcessRead(SinkRecord record, string stateKey)
    {
        var stateValue = ReadFromCache(stateKey);
        var merged = MergeStateIntoRecord(record, stateValue);

        EmitRecord(GetKeyString(record), merged, ConvertHeaders(record.Headers));
    }

    private void ProcessWrite(SinkRecord record, string stateKey)
    {
        var stateValue = ExtractField(record, _valueField);
        WriteToCache(stateKey, stateValue ?? "");

        // Also emit to state topic for persistence
        if (!string.IsNullOrEmpty(_stateTopic))
        {
            EmitRecordTo(_stateTopic, stateKey, stateValue);
        }

        EmitRecord(GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
    }

    private void ProcessReadWrite(SinkRecord record, string stateKey)
    {
        // Read first
        var existingState = ReadFromCache(stateKey);

        // Write updated value
        var newValue = ExtractField(record, _valueField);
        if (newValue is not null)
        {
            WriteToCache(stateKey, newValue);

            if (!string.IsNullOrEmpty(_stateTopic))
            {
                EmitRecordTo(_stateTopic, stateKey, newValue);
            }
        }

        // Merge existing state into record and emit
        var merged = MergeStateIntoRecord(record, existingState);
        EmitRecord(GetKeyString(record), merged, ConvertHeaders(record.Headers));
    }

    private void BootstrapFromStateTopic(SinkRecord record)
    {
        var key = record.Key is not null ? Encoding.UTF8.GetString(record.Key) : null;
        if (string.IsNullOrEmpty(key))
            return;

        var value = Encoding.UTF8.GetString(record.Value);
        WriteToCache(key, value);
    }

    private string? ReadFromCache(string key)
    {
        if (!_cache.TryGetValue(key, out var entry))
            return null;

        // Check TTL
        if (_ttlMs > 0)
        {
            var elapsed = (DateTimeOffset.UtcNow - entry.WrittenAt).TotalMilliseconds;
            if (elapsed >= _ttlMs)
            {
                _cache.TryRemove(key, out _);
                return null;
            }
        }

        return entry.Value;
    }

    private void WriteToCache(string key, string value)
    {
        _cache[key] = new StateEntry(value, DateTimeOffset.UtcNow);
    }

    private string MergeStateIntoRecord(SinkRecord record, string? stateValue)
    {
        try
        {
            var recordObj = JsonNode.Parse(record.Value);

            if (recordObj is JsonObject jsonObj)
            {
                if (stateValue is not null)
                {
                    try
                    {
                        jsonObj[_outputField] = JsonNode.Parse(stateValue);
                    }
                    catch (JsonException)
                    {
                        jsonObj[_outputField] = stateValue;
                    }
                }
                else
                {
                    jsonObj[_outputField] = null;
                }

                return jsonObj.ToJsonString();
            }
        }
        catch (JsonException)
        {
            // Fall through
        }

        return Encoding.UTF8.GetString(record.Value);
    }

    private static string? ExtractField(SinkRecord record, string fieldPath)
    {
        if (string.IsNullOrEmpty(fieldPath))
            return null;

        try
        {
            using var doc = JsonDocument.Parse(record.Value);
            var parts = fieldPath.TrimStart('$', '.').Split('.');
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
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                JsonValueKind.Null => null,
                _ => current.GetRawText()
            };
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// Exposes cache count for testing.
    /// </summary>
    internal int CacheCount => _cache.Count;

    /// <summary>
    /// Reads a value from the cache directly for testing.
    /// </summary>
    internal string? GetCachedValue(string key) => ReadFromCache(key);
}

internal sealed class StateEntry(string value, DateTimeOffset writtenAt)
{
    public string Value { get; } = value;
    public DateTimeOffset WrittenAt { get; } = writtenAt;
}

namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Repartitions records to a target topic with a new partition key.
/// Supports static partition assignment, hash-based partitioning from a JSON field,
/// round-robin distribution, and topic rewriting.
/// </summary>
[ConnectorMetadata(
    Name = "Repartition",
    Description = "Republish records to a different topic or partition",
    Tags = "logic,repartition,route,partition,republish")]
public sealed class RepartitionNode : ProcessorConnector
{
    public override Type TaskClass => typeof(RepartitionNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Target topic for repartitioned records")
        .Define("partition.strategy", ConfigType.String, "key-hash", Importance.High,
            "Partitioning strategy: 'key-hash', 'field-hash', 'static', 'round-robin'")
        .Define("partition.field", ConfigType.String, "", Importance.Medium,
            "JSONPath for field-hash strategy (e.g., $.region)")
        .Define("partition.static", ConfigType.Int, "0", Importance.Medium,
            "Static partition number (for 'static' strategy)")
        .Define("partition.count", ConfigType.Int, "0", Importance.Medium,
            "Number of partitions for hash distribution (0 = auto/no explicit partition)")
        .Define("key.field", ConfigType.String, "", Importance.Low,
            "JSONPath for new record key (empty = keep original key)")
        .Define("preserve.headers", ConfigType.Boolean, "true", Importance.Low,
            "Whether to preserve original record headers");
}

internal sealed class RepartitionNodeTask : ProcessorTask
{
    private string _strategy = "key-hash";
    private string _partitionField = "";
    private int _staticPartition;
    private int _partitionCount;
    private string _keyField = "";
    private bool _preserveHeaders = true;
    private int _roundRobinCounter;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _strategy = config.TryGetValue("partition.strategy", out var s) ? s.ToLowerInvariant() : "key-hash";
        _partitionField = config.TryGetValue("partition.field", out var pf) ? pf : "";
        if (config.TryGetValue("partition.static", out var sp) && int.TryParse(sp, out var staticPart))
            _staticPartition = staticPart;
        if (config.TryGetValue("partition.count", out var pc) && int.TryParse(pc, out var partCount))
            _partitionCount = partCount;
        _keyField = config.TryGetValue("key.field", out var kf) ? kf : "";
        if (config.TryGetValue("preserve.headers", out var ph) && bool.TryParse(ph, out var preserve))
            _preserveHeaders = preserve;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var key = ResolveKey(record);
            var partition = ResolvePartition(record, key);
            var headers = _preserveHeaders ? ConvertHeaders(record.Headers) : null;

            if (partition.HasValue)
            {
                EmitRecordTo(OutputTopic, partition, key, record.Value, headers);
            }
            else
            {
                EmitRecordTo(OutputTopic, key, record.Value, headers);
            }
        }

        return Task.CompletedTask;
    }

    private string? ResolveKey(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_keyField))
            return GetKeyString(record);

        return ExtractJsonField(record, _keyField) ?? GetKeyString(record);
    }

    private int? ResolvePartition(SinkRecord record, string? key)
    {
        switch (_strategy)
        {
            case "static":
                return _staticPartition;

            case "round-robin":
                if (_partitionCount <= 0)
                    return null;
                return Interlocked.Increment(ref _roundRobinCounter) % _partitionCount;

            case "field-hash":
                if (string.IsNullOrEmpty(_partitionField) || _partitionCount <= 0)
                    return null;
                var fieldValue = ExtractJsonField(record, _partitionField);
                if (fieldValue is null)
                    return null;
                return (int)(((uint)fieldValue.GetHashCode(StringComparison.Ordinal)) % (uint)_partitionCount);

            case "key-hash":
            default:
                if (_partitionCount <= 0 || key is null || key.Length == 0)
                    return null;
                return (int)(((uint)key.GetHashCode(StringComparison.Ordinal)) % (uint)_partitionCount);
        }
    }

    private static string? ExtractJsonField(SinkRecord record, string path)
    {
        if (record.Value is null || record.Value.Length == 0)
            return null;

        try
        {
            using var doc = JsonDocument.Parse(record.Value);
            var element = ConditionEvaluator.GetJsonPath(doc.RootElement, path);
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
}

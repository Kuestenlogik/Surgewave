namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Timestamp converter node that converts timestamp fields between string, unix seconds, and unix milliseconds formats.
/// </summary>
[ConnectorMetadata(
    Name = "TimestampConverter",
    Description = "Convert timestamp fields between string, unix, and unix_ms formats",
    Tags = "transform,timestamp,convert,date,time")]
public sealed class TimestampConverterNode : ProcessorConnector
{
    public override Type TaskClass => typeof(TimestampConverterNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for converted records")
        .Define("target.type", ConfigType.String, "string", Importance.High,
            "Target timestamp format: 'string', 'unix' (seconds), or 'unix_ms' (milliseconds)")
        .Define("format", ConfigType.String, "yyyy-MM-ddTHH:mm:ssZ", Importance.Medium,
            "Date format pattern for string conversion (default: ISO 8601)")
        .Define("field", ConfigType.String, "", Importance.Medium,
            "JSON field containing the timestamp. If empty, uses record.Timestamp");
}

internal sealed class TimestampConverterNodeTask : ProcessorTask
{
    private string _targetType = "string";
    private string _format = "yyyy-MM-ddTHH:mm:ssZ";
    private string _field = "";

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _targetType = config.TryGetValue("target.type", out var t) ? t : "string";
        _format = config.TryGetValue("format", out var f) && !string.IsNullOrEmpty(f)
            ? f
            : "yyyy-MM-ddTHH:mm:ssZ";
        _field = config.TryGetValue("field", out var fld) ? fld : "";
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var converted = ConvertRecord(record);
            EmitRecord(GetKeyString(record), converted, ConvertHeaders(record.Headers));
        }

        return Task.CompletedTask;
    }

    private object ConvertRecord(SinkRecord record)
    {
        // No field specified: use record timestamp and inject into the value
        if (string.IsNullOrEmpty(_field))
        {
            return ConvertRecordTimestamp(record);
        }

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return record.Value;

        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            return record.Value;

        var root = JsonNode.Parse(doc.RootElement.GetRawText()) as JsonObject;
        if (root is null)
            return record.Value;

        if (root[_field] is not JsonNode fieldNode)
            return record.Value;

        var timestamp = ParseTimestamp(fieldNode);
        if (timestamp is null)
            return record.Value;

        root[_field] = FormatTimestamp(timestamp.Value);
        return root.ToJsonString();
    }

    private string ConvertRecordTimestamp(SinkRecord record)
    {
        var converted = FormatTimestamp(record.Timestamp);

        using var doc = ParseJsonValue(record);
        if (doc is not null && doc.RootElement.ValueKind == JsonValueKind.Object)
        {
            var root = JsonNode.Parse(doc.RootElement.GetRawText()) as JsonObject;
            if (root is not null)
            {
                root["timestamp"] = converted;
                return root.ToJsonString();
            }
        }

        // Non-JSON or non-object: return just the converted timestamp
        return converted.ToJsonString();
    }

    private DateTimeOffset? ParseTimestamp(JsonNode node)
    {
        var kind = node.GetValueKind();

        if (kind == JsonValueKind.Number)
        {
            var numericValue = node.GetValue<double>();

            // Auto-detect: if > 1e12 treat as milliseconds, otherwise seconds
            if (numericValue > 1e12)
            {
                return DateTimeOffset.FromUnixTimeMilliseconds((long)numericValue);
            }

            return DateTimeOffset.FromUnixTimeSeconds((long)numericValue);
        }

        if (kind == JsonValueKind.String)
        {
            var strValue = node.GetValue<string>();
            if (DateTimeOffset.TryParseExact(strValue, _format, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
            {
                return parsed;
            }

            // Fallback: try general ISO parse
            if (DateTimeOffset.TryParse(strValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out var fallback))
            {
                return fallback;
            }
        }

        return null;
    }

    private JsonValue FormatTimestamp(DateTimeOffset timestamp)
    {
        return _targetType switch
        {
            "unix" => JsonValue.Create(timestamp.ToUnixTimeSeconds()),
            "unix_ms" => JsonValue.Create(timestamp.ToUnixTimeMilliseconds()),
            _ => JsonValue.Create(timestamp.ToString(_format, CultureInfo.InvariantCulture))
        };
    }
}

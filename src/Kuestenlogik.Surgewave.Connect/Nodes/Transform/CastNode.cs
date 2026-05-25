namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Cast node that converts JSON field values to specified data types.
/// </summary>
[ConnectorMetadata(
    Name = "Cast",
    Description = "Cast JSON field values to specified data types",
    Tags = "transform,cast,type,convert")]
public sealed class CastNode : ProcessorConnector
{
    public override Type TaskClass => typeof(CastNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for cast records")
        .Define("cast.spec", ConfigType.String, "", Importance.High,
            "Comma-separated field:type pairs (e.g., age:int32,price:float64,active:boolean,name:string)");
}

internal sealed class CastNodeTask : ProcessorTask
{
    private Dictionary<string, string> _castSpecs = new(StringComparer.Ordinal);

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);

        if (config.TryGetValue("cast.spec", out var spec) && !string.IsNullOrWhiteSpace(spec))
        {
            foreach (var pair in spec.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                var parts = pair.Split(':', 2);
                if (parts.Length == 2)
                {
                    _castSpecs[parts[0].Trim()] = parts[1].Trim().ToLowerInvariant();
                }
            }
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var casted = CastRecord(record);
            EmitRecord(GetKeyString(record), casted, ConvertHeaders(record.Headers));
        }

        return Task.CompletedTask;
    }

    private object CastRecord(SinkRecord record)
    {
        if (_castSpecs.Count == 0)
            return record.Value;

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return record.Value;

        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            return record.Value;

        var root = JsonNode.Parse(doc.RootElement.GetRawText()) as JsonObject;
        if (root is null)
            return record.Value;

        foreach (var (field, targetType) in _castSpecs)
        {
            if (root[field] is JsonNode node)
            {
                var converted = ConvertValue(node, targetType);
                if (converted is not null)
                {
                    root[field] = converted;
                }
            }
        }

        return root.ToJsonString();
    }

    private static JsonNode? ConvertValue(JsonNode node, string targetType)
    {
        var rawValue = GetRawValue(node);

        return targetType switch
        {
            "int32" => JsonValue.Create(Convert.ToInt32(rawValue, CultureInfo.InvariantCulture)),
            "int64" => JsonValue.Create(Convert.ToInt64(rawValue, CultureInfo.InvariantCulture)),
            "float32" => JsonValue.Create(Convert.ToSingle(rawValue, CultureInfo.InvariantCulture)),
            "float64" => JsonValue.Create(Convert.ToDouble(rawValue, CultureInfo.InvariantCulture)),
            "boolean" => JsonValue.Create(ConvertToBoolean(rawValue)),
            "string" => JsonValue.Create(Convert.ToString(rawValue, CultureInfo.InvariantCulture) ?? ""),
            _ => node
        };
    }

    private static object GetRawValue(JsonNode node)
    {
        var kind = node.GetValueKind();
        return kind switch
        {
            JsonValueKind.String => node.GetValue<string>(),
            JsonValueKind.Number => node.GetValue<double>(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => node.ToJsonString()
        };
    }

    private static bool ConvertToBoolean(object value)
    {
        return value switch
        {
            bool b => b,
            string s => s.Length > 0
                && !string.Equals(s, "false", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(s, "0", StringComparison.Ordinal),
            double d => d != 0.0,
            _ => Convert.ToBoolean(value, CultureInfo.InvariantCulture)
        };
    }
}

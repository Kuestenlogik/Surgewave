namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

/// <summary>
/// Value-to-key node that extracts fields from the record value to form the record key.
/// </summary>
[ConnectorMetadata(
    Name = "ValueToKey",
    Description = "Extract fields from value to use as record key",
    Tags = "transform,key,extract,value")]
public sealed class ValueToKeyNode : ProcessorConnector
{
    public override Type TaskClass => typeof(ValueToKeyNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for rekeyed records")
        .Define("fields", ConfigType.String, "", Importance.High,
            "Comma-separated field names to extract as key");
}

internal sealed class ValueToKeyNodeTask : ProcessorTask
{
    private string[] _fields = [];

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);

        if (config.TryGetValue("fields", out var fields) && !string.IsNullOrWhiteSpace(fields))
        {
            _fields = fields.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var newKey = ExtractKey(record);
            EmitRecord(newKey, record.Value, ConvertHeaders(record.Headers));
        }

        return Task.CompletedTask;
    }

    private string? ExtractKey(SinkRecord record)
    {
        if (_fields.Length == 0)
            return GetKeyString(record);

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return GetKeyString(record);

        if (_fields.Length == 1)
        {
            var element = ConditionEvaluator.GetJsonPath(doc.RootElement, "$." + _fields[0]);
            if (element.ValueKind == JsonValueKind.Undefined)
                return GetKeyString(record);

            return element.ValueKind == JsonValueKind.String
                ? element.GetString()
                : element.GetRawText();
        }

        // Multiple fields: build JSON object
        var keyObj = new JsonObject();
        foreach (var field in _fields)
        {
            var element = ConditionEvaluator.GetJsonPath(doc.RootElement, "$." + field);
            if (element.ValueKind != JsonValueKind.Undefined)
            {
                keyObj[field] = JsonNode.Parse(element.GetRawText());
            }
        }

        return keyObj.ToJsonString();
    }
}

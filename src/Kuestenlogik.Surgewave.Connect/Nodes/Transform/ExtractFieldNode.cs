namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Configuration;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

/// <summary>
/// Extract field node that replaces the entire record value with a single extracted field.
/// </summary>
[ConnectorMetadata(
    Name = "ExtractField",
    Description = "Extract a single field as the entire record value",
    Tags = "transform,extract,field,unwrap")]
public sealed class ExtractFieldNode : ProcessorConnector
{
    public override Type TaskClass => typeof(ExtractFieldNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for extracted records")
        .Define("extract.field", ConfigType.String, "", Importance.High,
            "JSONPath to the field to extract (e.g., $.user.name)");
}

internal sealed class ExtractFieldNodeTask : ProcessorTask
{
    private string _extractField = "";

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _extractField = config.TryGetValue("extract.field", out var f) ? f : "";
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var extracted = ExtractValue(record);
            if (extracted is not null)
            {
                EmitRecord(GetKeyString(record), extracted, ConvertHeaders(record.Headers));
            }
        }

        return Task.CompletedTask;
    }

    private string? ExtractValue(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_extractField))
            return null;

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return null;

        var element = ConditionEvaluator.GetJsonPath(doc.RootElement, _extractField);
        if (element.ValueKind == JsonValueKind.Undefined)
            return null;

        // Return the raw value: strings without quotes, others as raw JSON
        return element.ValueKind == JsonValueKind.String
            ? element.GetString()
            : element.GetRawText();
    }
}

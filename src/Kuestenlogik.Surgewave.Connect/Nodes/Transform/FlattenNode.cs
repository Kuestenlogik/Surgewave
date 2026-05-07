namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Flatten node that recursively flattens nested JSON objects into a single-level structure.
/// </summary>
[ConnectorMetadata(
    Name = "Flatten",
    Description = "Flatten nested JSON objects into a single-level structure",
    Tags = "transform,flatten,nested,json")]
public sealed class FlattenNode : ProcessorConnector
{
    public override Type TaskClass => typeof(FlattenNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for flattened records")
        .Define("flatten.delimiter", ConfigType.String, ".", Importance.Medium,
            "Delimiter for flattened key names (default '.')");
}

internal sealed class FlattenNodeTask : ProcessorTask
{
    private string _delimiter = ".";

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _delimiter = config.TryGetValue("flatten.delimiter", out var d) ? d : ".";
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var flattened = FlattenRecord(record);
            EmitRecord(GetKeyString(record), flattened, ConvertHeaders(record.Headers));
        }

        return Task.CompletedTask;
    }

    private object FlattenRecord(SinkRecord record)
    {
        using var doc = ParseJsonValue(record);
        if (doc is null)
            return record.Value;

        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            return record.Value;

        var result = new JsonObject();
        FlattenElement(doc.RootElement, "", result);
        return result.ToJsonString();
    }

    private void FlattenElement(JsonElement element, string prefix, JsonObject result)
    {
        foreach (var property in element.EnumerateObject())
        {
            var key = string.IsNullOrEmpty(prefix)
                ? property.Name
                : prefix + _delimiter + property.Name;

            if (property.Value.ValueKind == JsonValueKind.Object)
            {
                FlattenElement(property.Value, key, result);
            }
            else
            {
                result[key] = JsonNode.Parse(property.Value.GetRawText());
            }
        }
    }
}

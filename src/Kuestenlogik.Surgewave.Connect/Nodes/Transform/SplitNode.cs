namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Configuration;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

/// <summary>
/// Split node that explodes array fields into individual records.
/// </summary>
[ConnectorMetadata(
    Name = "Split",
    Description = "Split array into individual records",
    Tags = "transform,split,explode,flatten")]
public sealed class SplitNode : ProcessorConnector
{
    public override Type TaskClass => typeof(SplitNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for split records")
        .Define("array.path", ConfigType.String, "$", Importance.High,
            "JSONPath to array field")
        .Define("keep.parent", ConfigType.Boolean, "true", Importance.Medium,
            "Include parent fields in split records");
}

internal sealed class SplitNodeTask : ProcessorTask
{
    private string _arrayPath = "$";
    private bool _keepParent = true;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _arrayPath = config.TryGetValue("array.path", out var p) ? p : "$";
        _keepParent = !config.TryGetValue("keep.parent", out var k) || !bool.TryParse(k, out var b) || b;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var splitRecords = SplitRecord(record);
            var headers = ConvertHeaders(record.Headers);

            foreach (var split in splitRecords)
            {
                EmitRecord(GetKeyString(record), split, headers);
            }
        }

        return Task.CompletedTask;
    }

    private IEnumerable<string> SplitRecord(SinkRecord record)
    {
        using var doc = ParseJsonValue(record);
        if (doc is null)
            yield break;

        var array = ConditionEvaluator.GetJsonPath(doc.RootElement, _arrayPath);
        if (array.ValueKind != JsonValueKind.Array)
        {
            yield return System.Text.Encoding.UTF8.GetString(record.Value);
            yield break;
        }

        Dictionary<string, JsonElement>? parentFields = null;
        if (_keepParent && _arrayPath != "$")
        {
            parentFields = GetParentFields(doc.RootElement, _arrayPath);
        }

        var index = 0;
        foreach (var element in array.EnumerateArray())
        {
            if (_keepParent && parentFields is not null)
            {
                var merged = MergeWithParent(element, parentFields, index);
                yield return merged;
            }
            else
            {
                yield return element.GetRawText();
            }
            index++;
        }
    }

    private static Dictionary<string, JsonElement> GetParentFields(JsonElement root, string arrayPath)
    {
        var result = new Dictionary<string, JsonElement>();
        var arrayFieldName = arrayPath.Split('.').LastOrDefault()?.TrimStart('$') ?? "";

        if (root.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.Name != arrayFieldName)
                {
                    result[prop.Name] = prop.Value;
                }
            }
        }

        return result;
    }

    private static string MergeWithParent(JsonElement element, Dictionary<string, JsonElement> parentFields, int index)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        writer.WriteStartObject();

        foreach (var (key, value) in parentFields)
        {
            writer.WritePropertyName(key);
            value.WriteTo(writer);
        }

        writer.WriteNumber("_index", index);

        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in element.EnumerateObject())
            {
                writer.WritePropertyName(prop.Name);
                prop.Value.WriteTo(writer);
            }
        }
        else
        {
            writer.WritePropertyName("item");
            element.WriteTo(writer);
        }

        writer.WriteEndObject();
        writer.Flush();

        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

}

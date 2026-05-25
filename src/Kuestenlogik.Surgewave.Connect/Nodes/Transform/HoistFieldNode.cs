namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Hoist field node that wraps the entire record value inside a new JSON object at the configured field name.
/// </summary>
[ConnectorMetadata(
    Name = "HoistField",
    Description = "Wrap the entire record value inside a new JSON object at a specified field",
    Tags = "transform,hoist,wrap,field")]
public sealed class HoistFieldNode : ProcessorConnector
{
    public override Type TaskClass => typeof(HoistFieldNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for hoisted records")
        .Define("hoist.field", ConfigType.String, "", Importance.High,
            "Field name under which to nest the entire record value");
}

internal sealed class HoistFieldNodeTask : ProcessorTask
{
    private string _hoistField = "";

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _hoistField = config.TryGetValue("hoist.field", out var f) ? f : "";
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var hoisted = HoistRecord(record);
            EmitRecord(GetKeyString(record), hoisted, ConvertHeaders(record.Headers));
        }

        return Task.CompletedTask;
    }

    private object HoistRecord(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_hoistField))
            return record.Value;

        var wrapper = new JsonObject();

        using var doc = ParseJsonValue(record);
        if (doc is not null)
        {
            // JSON value: nest the parsed JSON node
            wrapper[_hoistField] = JsonNode.Parse(doc.RootElement.GetRawText());
        }
        else
        {
            // Non-JSON value: wrap as string
            var textValue = Encoding.UTF8.GetString(record.Value);
            wrapper[_hoistField] = textValue;
        }

        return wrapper.ToJsonString();
    }
}

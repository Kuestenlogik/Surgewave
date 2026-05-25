namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Insert field node that injects record metadata and static values into JSON records.
/// </summary>
[ConnectorMetadata(
    Name = "InsertField",
    Description = "Insert metadata or static fields into records",
    Tags = "transform,insert,field,metadata")]
public sealed class InsertFieldNode : ProcessorConnector
{
    public override Type TaskClass => typeof(InsertFieldNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for enriched records")
        .Define("offset.field", ConfigType.String, "", Importance.Medium,
            "Field name to insert record offset")
        .Define("partition.field", ConfigType.String, "", Importance.Medium,
            "Field name to insert record partition")
        .Define("timestamp.field", ConfigType.String, "", Importance.Medium,
            "Field name to insert record timestamp")
        .Define("topic.field", ConfigType.String, "", Importance.Medium,
            "Field name to insert record topic")
        .Define("static.field", ConfigType.String, "", Importance.Medium,
            "Field name for a static value")
        .Define("static.value", ConfigType.String, "", Importance.Medium,
            "Static value to insert");
}

internal sealed class InsertFieldNodeTask : ProcessorTask
{
    private string _offsetField = "";
    private string _partitionField = "";
    private string _timestampField = "";
    private string _topicField = "";
    private string _staticField = "";
    private string _staticValue = "";

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _offsetField = config.TryGetValue("offset.field", out var o) ? o : "";
        _partitionField = config.TryGetValue("partition.field", out var p) ? p : "";
        _timestampField = config.TryGetValue("timestamp.field", out var t) ? t : "";
        _topicField = config.TryGetValue("topic.field", out var tp) ? tp : "";
        _staticField = config.TryGetValue("static.field", out var sf) ? sf : "";
        _staticValue = config.TryGetValue("static.value", out var sv) ? sv : "";
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var enriched = EnrichRecord(record);
            EmitRecord(GetKeyString(record), enriched, ConvertHeaders(record.Headers));
        }

        return Task.CompletedTask;
    }

    private object EnrichRecord(SinkRecord record)
    {
        using var doc = ParseJsonValue(record);
        if (doc is null)
            return record.Value;

        var result = new JsonObject();

        if (doc.RootElement.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                result[prop.Name] = JsonNode.Parse(prop.Value.GetRawText());
            }
        }

        if (!string.IsNullOrEmpty(_offsetField))
            result[_offsetField] = record.Offset;

        if (!string.IsNullOrEmpty(_partitionField))
            result[_partitionField] = record.Partition;

        if (!string.IsNullOrEmpty(_timestampField))
            result[_timestampField] = record.Timestamp.ToString("o");

        if (!string.IsNullOrEmpty(_topicField))
            result[_topicField] = record.Topic;

        if (!string.IsNullOrEmpty(_staticField))
            result[_staticField] = _staticValue;

        return result.ToJsonString();
    }
}

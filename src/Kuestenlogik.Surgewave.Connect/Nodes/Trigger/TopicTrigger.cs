namespace Kuestenlogik.Surgewave.Connect.Nodes.Trigger;

using Kuestenlogik.Surgewave.Connect.Configuration;
using Kuestenlogik.Surgewave.Connect.Nodes;

/// <summary>
/// Topic trigger that reads from a Surgewave topic and forwards records.
/// Used as a pipeline entry point.
/// </summary>
[ConnectorMetadata(
    Name = "TopicTrigger",
    Description = "Surgewave topic as event source",
    Tags = "trigger,topic,source")]
public sealed class TopicTrigger : ProcessorConnector
{
    public override Type TaskClass => typeof(TopicTriggerTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("topics", ConfigType.String, "", Importance.High,
            "Source topics to read from")
        .Define("output.topic", ConfigType.String, "", Importance.Medium,
            "Output topic if different from input")
        .Define("add.source.metadata", ConfigType.Boolean, "true", Importance.Low,
            "Add source topic/partition headers");
}

internal sealed class TopicTriggerTask : ProcessorTask
{
    private bool _addSourceMetadata = true;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _addSourceMetadata = !config.TryGetValue("add.source.metadata", out var v) || !bool.TryParse(v, out var b) || b;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var headers = ConvertHeaders(record.Headers) ?? [];

            if (_addSourceMetadata)
            {
                headers["_source_topic"] = record.Topic;
                headers["_source_partition"] = record.Partition.ToString();
                headers["_source_offset"] = record.Offset.ToString();
                headers["_trigger_time"] = DateTimeOffset.UtcNow.ToString("O");
            }

            EmitRecord(GetKeyString(record), record.Value, headers);
        }

        return Task.CompletedTask;
    }
}

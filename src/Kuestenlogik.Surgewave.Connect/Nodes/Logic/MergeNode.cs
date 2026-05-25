namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Merge node that combines records from multiple input topics into a single output.
/// </summary>
[ConnectorMetadata(
    Name = "Merge",
    Description = "Combine multiple inputs into one",
    Tags = "logic,merge,combine,union")]
public sealed class MergeNode : ProcessorConnector
{
    public override Type TaskClass => typeof(MergeNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for merged records")
        .Define("add.source.header", ConfigType.Boolean, "false", Importance.Low,
            "Add header indicating source topic");
}

internal sealed class MergeNodeTask : ProcessorTask
{
    private bool _addSourceHeader;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _addSourceHeader = config.TryGetValue("add.source.header", out var v) && bool.TryParse(v, out var b) && b;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var headers = ConvertHeaders(record.Headers);

            if (_addSourceHeader)
            {
                headers ??= [];
                headers["_source_topic"] = record.Topic;
                headers["_source_partition"] = record.Partition.ToString();
            }

            EmitRecord(GetKeyString(record), record.Value, headers);
        }

        return Task.CompletedTask;
    }
}

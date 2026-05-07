namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Timestamp router node that routes records to topics based on record timestamp.
/// </summary>
[ConnectorMetadata(
    Name = "TimestampRouter",
    Description = "Route records to topics based on timestamp formatting",
    Tags = "transform,route,timestamp,topic")]
public sealed class TimestampRouterNode : ProcessorConnector
{
    public override Type TaskClass => typeof(TimestampRouterNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.Low,
            "Fallback output topic (not typically used)")
        .Define("topic.format", ConfigType.String, "${topic}-${timestamp}", Importance.High,
            "Topic format template with ${topic} and ${timestamp} placeholders")
        .Define("timestamp.format", ConfigType.String, "yyyyMMdd", Importance.High,
            "Timestamp format string (e.g., yyyyMMdd, yyyy-MM-dd)");
}

internal sealed class TimestampRouterNodeTask : ProcessorTask
{
    private string _topicFormat = "${topic}-${timestamp}";
    private string _timestampFormat = "yyyyMMdd";

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _topicFormat = config.TryGetValue("topic.format", out var tf) ? tf : "${topic}-${timestamp}";
        _timestampFormat = config.TryGetValue("timestamp.format", out var tsf) ? tsf : "yyyyMMdd";
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var targetTopic = ComputeTargetTopic(record);

            if (!string.IsNullOrEmpty(targetTopic))
            {
                EmitRecordTo(targetTopic, GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            }
        }

        return Task.CompletedTask;
    }

    private string ComputeTargetTopic(SinkRecord record)
    {
        var formattedTimestamp = record.Timestamp.ToString(_timestampFormat);
        return _topicFormat
            .Replace("${topic}", record.Topic)
            .Replace("${timestamp}", formattedTimestamp);
    }
}

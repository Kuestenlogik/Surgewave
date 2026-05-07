namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Regex router node that routes records to topics based on regex replacement of the source topic.
/// </summary>
[ConnectorMetadata(
    Name = "RegexRouter",
    Description = "Route records to topics using regex pattern matching on the source topic",
    Tags = "transform,route,regex,topic")]
public sealed class RegexRouterNode : ProcessorConnector
{
    public override Type TaskClass => typeof(RegexRouterNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.Low,
            "Fallback output topic when no regex pattern is configured")
        .Define("regex.pattern", ConfigType.String, "", Importance.High,
            "Regex pattern to match against the record topic")
        .Define("regex.replacement", ConfigType.String, "", Importance.High,
            "Replacement string (supports capture groups $1, $2, etc.)");
}

internal sealed class RegexRouterNodeTask : ProcessorTask
{
    private Regex? _pattern;
    private string _replacement = "";

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);

        if (config.TryGetValue("regex.pattern", out var pattern) && !string.IsNullOrEmpty(pattern))
        {
            _pattern = new Regex(pattern, RegexOptions.Compiled);
        }

        _replacement = config.TryGetValue("regex.replacement", out var r) ? r : "";
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var targetTopic = ComputeTargetTopic(record.Topic);

            if (!string.IsNullOrEmpty(targetTopic))
            {
                EmitRecordTo(targetTopic, GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            }
        }

        return Task.CompletedTask;
    }

    private string? ComputeTargetTopic(string sourceTopic)
    {
        if (_pattern is null)
            return OutputTopic;

        return _pattern.Replace(sourceTopic, _replacement);
    }
}

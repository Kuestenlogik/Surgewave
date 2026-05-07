namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Multi-branch routing node based on field value.
/// Routes records to different topics based on a discriminator field.
/// </summary>
[ConnectorMetadata(
    Name = "Switch",
    Description = "Multi-way branch based on field value",
    Tags = "logic,switch,route,branch")]
public sealed class SwitchNode : ProcessorConnector
{
    public override Type TaskClass => typeof(SwitchNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("discriminator", ConfigType.String, "$.type", Importance.High,
            "JSONPath to discriminator field")
        .Define("default.topic", ConfigType.String, "", Importance.Medium,
            "Default topic for unmatched records");
}

internal sealed class SwitchNodeTask : ProcessorTask
{
    private string _discriminator = "";
    private string _defaultTopic = "";
    private readonly Dictionary<string, string> _cases = new(StringComparer.OrdinalIgnoreCase);

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _discriminator = config.TryGetValue("discriminator", out var d) ? d : "$.type";
        _defaultTopic = config.TryGetValue("default.topic", out var dt) ? dt : "";

        foreach (var (key, value) in config)
        {
            if (key.StartsWith("case.", StringComparison.OrdinalIgnoreCase))
            {
                var caseValue = key[5..];
                _cases[caseValue] = value;
            }
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var discriminatorValue = GetDiscriminatorValue(record);
            var targetTopic = GetTargetTopic(discriminatorValue);

            if (!string.IsNullOrEmpty(targetTopic))
            {
                EmitRecordTo(targetTopic, GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            }
        }

        return Task.CompletedTask;
    }

    private string? GetDiscriminatorValue(SinkRecord record)
    {
        using var doc = ParseJsonValue(record);
        if (doc is null)
            return null;

        var element = ConditionEvaluator.GetJsonPath(doc.RootElement, _discriminator);
        if (element.ValueKind == JsonValueKind.Undefined)
            return null;

        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null
        };
    }

    private string GetTargetTopic(string? discriminatorValue)
    {
        if (discriminatorValue is not null && _cases.TryGetValue(discriminatorValue, out var topic))
            return topic;

        return _defaultTopic;
    }
}

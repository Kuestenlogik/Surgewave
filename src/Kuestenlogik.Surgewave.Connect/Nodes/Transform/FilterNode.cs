namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using Kuestenlogik.Surgewave.Connect.Configuration;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

/// <summary>
/// Filter node that passes through only records matching a condition.
/// </summary>
[ConnectorMetadata(
    Name = "Filter",
    Description = "Filter records by condition",
    Tags = "transform,filter,where")]
public sealed class FilterNode : ProcessorConnector
{
    public override Type TaskClass => typeof(FilterNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for filtered records")
        .Define("condition", ConfigType.String, "", Importance.High,
            "Filter condition expression")
        .Define("negate", ConfigType.Boolean, "false", Importance.Low,
            "Negate the condition (pass non-matching)");
}

internal sealed class FilterNodeTask : ProcessorTask
{
    private string _condition = "";
    private bool _negate;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _condition = config.TryGetValue("condition", out var c) ? c : "";
        _negate = config.TryGetValue("negate", out var n) && bool.TryParse(n, out var b) && b;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var matches = EvaluateCondition(record);
            var shouldPass = _negate ? !matches : matches;

            if (shouldPass)
            {
                EmitRecord(GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            }
        }

        return Task.CompletedTask;
    }

    private bool EvaluateCondition(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_condition))
            return true;

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return false;

        return ConditionEvaluator.Evaluate(_condition, doc.RootElement);
    }
}

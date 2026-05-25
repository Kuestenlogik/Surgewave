namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Text.Json;
using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Conditional branching node that routes records based on a condition.
/// Outputs to either 'true' or 'false' topic depending on evaluation.
/// </summary>
[ConnectorMetadata(
    Name = "If",
    Description = "Conditional branch based on expression",
    Tags = "logic,if,condition,branch")]
public sealed class IfNode : ProcessorConnector
{
    public override Type TaskClass => typeof(IfNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("condition", ConfigType.String, "", Importance.High,
            "Condition expression (e.g., $.status == 'active')")
        .Define("output.true.topic", ConfigType.String, "", Importance.High,
            "Topic for matching records")
        .Define("output.false.topic", ConfigType.String, "", Importance.High,
            "Topic for non-matching records");
}

internal sealed class IfNodeTask : ProcessorTask
{
    private string _condition = "";
    private string _trueTopic = "";
    private string _falseTopic = "";

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _condition = config.TryGetValue("condition", out var c) ? c : "";
        _trueTopic = config.TryGetValue("output.true.topic", out var t) ? t : "";
        _falseTopic = config.TryGetValue("output.false.topic", out var f) ? f : "";
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var result = EvaluateCondition(record);
            var targetTopic = result ? _trueTopic : _falseTopic;

            if (!string.IsNullOrEmpty(targetTopic))
            {
                EmitRecordTo(targetTopic, GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
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

/// <summary>
/// Simple condition evaluator supporting JSONPath-like expressions.
/// </summary>
internal static partial class ConditionEvaluator
{
    public static bool Evaluate(string condition, JsonElement root)
    {
        var match = ConditionPattern().Match(condition);
        if (!match.Success)
            return false;

        var path = match.Groups["path"].Value;
        var op = match.Groups["op"].Value;
        var value = match.Groups["value"].Value.Trim('\'', '"');

        var element = GetJsonPath(root, path);
        if (element.ValueKind == JsonValueKind.Undefined)
            return false;

        return op switch
        {
            "==" or "=" => CompareEqual(element, value),
            "!=" or "<>" => !CompareEqual(element, value),
            ">" => CompareGreater(element, value),
            "<" => CompareLess(element, value),
            ">=" => CompareGreater(element, value) || CompareEqual(element, value),
            "<=" => CompareLess(element, value) || CompareEqual(element, value),
            "contains" => element.GetString()?.Contains(value, StringComparison.OrdinalIgnoreCase) ?? false,
            "startsWith" => element.GetString()?.StartsWith(value, StringComparison.OrdinalIgnoreCase) ?? false,
            "endsWith" => element.GetString()?.EndsWith(value, StringComparison.OrdinalIgnoreCase) ?? false,
            _ => false
        };
    }

    internal static JsonElement GetJsonPath(JsonElement root, string path)
    {
        var current = root;
        var parts = path.TrimStart('$', '.').Split('.');

        foreach (var part in parts)
        {
            if (string.IsNullOrEmpty(part))
                continue;

            if (current.ValueKind != JsonValueKind.Object)
                return default;

            if (!current.TryGetProperty(part, out var next))
                return default;

            current = next;
        }

        return current;
    }

    private static bool CompareEqual(JsonElement element, string value)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString() == value,
            JsonValueKind.Number => double.TryParse(value, out var d) && element.GetDouble() == d,
            JsonValueKind.True => value.Equals("true", StringComparison.OrdinalIgnoreCase),
            JsonValueKind.False => value.Equals("false", StringComparison.OrdinalIgnoreCase),
            JsonValueKind.Null => value.Equals("null", StringComparison.OrdinalIgnoreCase),
            _ => false
        };
    }

    private static bool CompareGreater(JsonElement element, string value)
    {
        if (element.ValueKind != JsonValueKind.Number || !double.TryParse(value, out var d))
            return false;
        return element.GetDouble() > d;
    }

    private static bool CompareLess(JsonElement element, string value)
    {
        if (element.ValueKind != JsonValueKind.Number || !double.TryParse(value, out var d))
            return false;
        return element.GetDouble() < d;
    }

    [GeneratedRegex(@"^\s*(?<path>\$[\w.]+)\s*(?<op>==|!=|<>|>=|<=|>|<|=|contains|startsWith|endsWith)\s*(?<value>.+)\s*$")]
    private static partial Regex ConditionPattern();
}

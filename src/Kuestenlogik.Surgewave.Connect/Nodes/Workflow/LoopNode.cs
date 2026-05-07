namespace Kuestenlogik.Surgewave.Connect.Nodes.Workflow;

using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Feedback loop node that sends records back to an earlier node until a condition is met or max iterations reached.
/// </summary>
[ConnectorMetadata(
    Name = "Loop",
    Description = "Iterate records back to an earlier node until a condition is met or max iterations reached",
    Tags = "workflow,loop,iteration,feedback",
    Icon = "Loop")]
public sealed class LoopNode : ProcessorConnector
{
    public override Type TaskClass => typeof(LoopNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("max.iterations", ConfigType.Int, "5", Importance.High,
            "Maximum loop iterations before forcing exit")
        .Define("condition.field", ConfigType.String, "", Importance.High,
            "JSON field to check for loop termination")
        .Define("condition.value", ConfigType.String, "", Importance.High,
            "Value that means 'done' (exit loop)")
        .Define("condition.operator", ConfigType.String, "equals", Importance.Medium,
            "Comparison operator: equals, not_equals, contains, greater_than, less_than")
        .Define("loop.topic", ConfigType.String, "", Importance.High,
            "Topic to send records back to (loop-back path)")
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Topic for records that pass the condition (exit path)")
        .Define("iteration.header", ConfigType.String, "x-loop-iteration", Importance.Low,
            "Header tracking iteration count");
}

internal sealed class LoopNodeTask : ProcessorTask
{
    private int _maxIterations = 5;
    private string _conditionField = "";
    private string _conditionValue = "";
    private string _conditionOperator = "equals";
    private string _loopTopic = "";
    private string _iterationHeader = "x-loop-iteration";

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        if (config.TryGetValue("max.iterations", out var mi) && int.TryParse(mi, out var maxIter))
            _maxIterations = maxIter;
        if (config.TryGetValue("condition.field", out var cf))
            _conditionField = cf;
        if (config.TryGetValue("condition.value", out var cv))
            _conditionValue = cv;
        if (config.TryGetValue("condition.operator", out var co))
            _conditionOperator = co;
        if (config.TryGetValue("loop.topic", out var lt))
            _loopTopic = lt;
        if (config.TryGetValue("iteration.header", out var ih))
            _iterationHeader = ih;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var iteration = GetIterationCount(record);
            var conditionMet = EvaluateCondition(record);

            var headers = ConvertHeaders(record.Headers) ?? [];
            var key = GetKeyString(record);

            if (conditionMet)
            {
                // Condition met — emit to output topic (exit loop)
                EmitRecord(key, record.Value, headers);
            }
            else if (iteration >= _maxIterations)
            {
                // Max iterations reached — force exit with exhausted marker
                headers["x-loop-exhausted"] = "true";
                headers[_iterationHeader] = iteration.ToString(CultureInfo.InvariantCulture);
                EmitRecord(key, record.Value, headers);
            }
            else
            {
                // Continue looping — increment iteration and send back
                headers[_iterationHeader] = (iteration + 1).ToString(CultureInfo.InvariantCulture);
                if (!string.IsNullOrEmpty(_loopTopic))
                {
                    EmitRecordTo(_loopTopic, key, record.Value, headers);
                }
            }
        }

        return Task.CompletedTask;
    }

    private int GetIterationCount(SinkRecord record)
    {
        if (record.Headers is null)
            return 0;

        if (record.Headers.TryGetValue(_iterationHeader, out var iterBytes))
        {
            var iterStr = Encoding.UTF8.GetString(iterBytes);
            if (int.TryParse(iterStr, out var count))
                return count;
        }

        return 0;
    }

    private bool EvaluateCondition(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_conditionField))
            return false;

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return false;

        var element = GetFieldValue(doc.RootElement, _conditionField);
        if (element.ValueKind == JsonValueKind.Undefined)
            return false;

        var fieldValue = element.ValueKind switch
        {
            JsonValueKind.String => element.GetString() ?? "",
            JsonValueKind.Number => element.GetDouble().ToString(CultureInfo.InvariantCulture),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => "null",
            _ => element.GetRawText()
        };

        return _conditionOperator switch
        {
            "equals" => string.Equals(fieldValue, _conditionValue, StringComparison.Ordinal),
            "not_equals" => !string.Equals(fieldValue, _conditionValue, StringComparison.Ordinal),
            "contains" => fieldValue.Contains(_conditionValue, StringComparison.OrdinalIgnoreCase),
            "greater_than" => double.TryParse(fieldValue, CultureInfo.InvariantCulture, out var gv) &&
                              double.TryParse(_conditionValue, CultureInfo.InvariantCulture, out var gc) && gv > gc,
            "less_than" => double.TryParse(fieldValue, CultureInfo.InvariantCulture, out var lv) &&
                           double.TryParse(_conditionValue, CultureInfo.InvariantCulture, out var lc) && lv < lc,
            _ => false
        };
    }

    private static JsonElement GetFieldValue(JsonElement root, string fieldPath)
    {
        var current = root;
        var parts = fieldPath.TrimStart('$', '.').Split('.');

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
}

namespace Kuestenlogik.Surgewave.Connect.Nodes.Workflow;

using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Routes records to multiple named output ports based on field values or broadcast to all.
/// Supports up to 10 named routes with a default fallback topic.
/// </summary>
[ConnectorMetadata(
    Name = "Multi Output",
    Description = "Route records to multiple named output ports based on conditions",
    Tags = "workflow,router,multi,output,fanout",
    Icon = "AccountTree")]
public sealed class MultiOutputNode : ProcessorConnector
{
    public override Type TaskClass => typeof(MultiOutputNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("route.field", ConfigType.String, "", Importance.High,
            "JSON field to evaluate for routing")
        .Define("route.1.value", ConfigType.String, "", Importance.Medium,
            "Value for route 1")
        .Define("route.1.topic", ConfigType.String, "", Importance.Medium,
            "Output topic for route 1")
        .Define("route.2.value", ConfigType.String, "", Importance.Medium,
            "Value for route 2")
        .Define("route.2.topic", ConfigType.String, "", Importance.Medium,
            "Output topic for route 2")
        .Define("default.topic", ConfigType.String, "", Importance.Medium,
            "Default output for unmatched records")
        .Define("broadcast", ConfigType.Boolean, "false", Importance.Medium,
            "If true, send to ALL routes (fan-out)");
}

internal sealed class MultiOutputNodeTask : ProcessorTask
{
    private string _routeField = "";
    private string _defaultTopic = "";
    private bool _broadcast;
    private readonly List<RouteEntry> _routes = [];

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        if (config.TryGetValue("route.field", out var rf))
            _routeField = rf;
        if (config.TryGetValue("default.topic", out var dt))
            _defaultTopic = dt;
        if (config.TryGetValue("broadcast", out var bc) && bool.TryParse(bc, out var broadcast))
            _broadcast = broadcast;

        // Parse route entries (route.1 through route.10)
        for (var i = 1; i <= 10; i++)
        {
            var valueKey = $"route.{i}.value";
            var topicKey = $"route.{i}.topic";

            if (config.TryGetValue(topicKey, out var topic) && !string.IsNullOrEmpty(topic))
            {
                var value = config.TryGetValue(valueKey, out var v) ? v : "";
                _routes.Add(new RouteEntry(value, topic));
            }
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var key = GetKeyString(record);
            var headers = ConvertHeaders(record.Headers);

            if (_broadcast)
            {
                // Fan-out: emit to all configured route topics
                foreach (var route in _routes)
                {
                    EmitRecordTo(route.Topic, key, record.Value, headers);
                }
            }
            else
            {
                // Route based on field value
                var fieldValue = ExtractFieldValue(record);
                var matched = false;

                foreach (var route in _routes)
                {
                    if (string.Equals(route.Value, fieldValue, StringComparison.Ordinal))
                    {
                        EmitRecordTo(route.Topic, key, record.Value, headers);
                        matched = true;
                        break;
                    }
                }

                if (!matched && !string.IsNullOrEmpty(_defaultTopic))
                {
                    EmitRecordTo(_defaultTopic, key, record.Value, headers);
                }
            }
        }

        return Task.CompletedTask;
    }

    private string? ExtractFieldValue(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_routeField))
            return null;

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return null;

        var parts = _routeField.TrimStart('$', '.').Split('.');
        var current = doc.RootElement;

        foreach (var part in parts)
        {
            if (string.IsNullOrEmpty(part))
                continue;

            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(part, out var next))
                return null;

            current = next;
        }

        return current.ValueKind switch
        {
            JsonValueKind.String => current.GetString(),
            JsonValueKind.Number => current.GetDouble().ToString(CultureInfo.InvariantCulture),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => "null",
            _ => current.GetRawText()
        };
    }
}

internal sealed class RouteEntry(string value, string topic)
{
    public string Value { get; } = value;
    public string Topic { get; } = topic;
}

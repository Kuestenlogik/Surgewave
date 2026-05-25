namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Header-from node that copies or moves JSON field values into record headers.
/// </summary>
[ConnectorMetadata(
    Name = "HeaderFrom",
    Description = "Copy or move JSON field values into record headers",
    Tags = "transform,header,field,extract")]
public sealed class HeaderFromNode : ProcessorConnector
{
    public override Type TaskClass => typeof(HeaderFromNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for transformed records")
        .Define("header.from.fields", ConfigType.String, "", Importance.High,
            "Comma-separated field names whose values become headers")
        .Define("header.from.headers", ConfigType.String, "", Importance.High,
            "Comma-separated header names to use (must match field count)")
        .Define("header.from.operation", ConfigType.String, "copy", Importance.Medium,
            "Operation: 'copy' keeps the field in the value, 'move' removes it");
}

internal sealed class HeaderFromNodeTask : ProcessorTask
{
    private string[] _fields = [];
    private string[] _headers = [];
    private bool _move;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);

        if (config.TryGetValue("header.from.fields", out var fields) && !string.IsNullOrWhiteSpace(fields))
        {
            _fields = fields.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        if (config.TryGetValue("header.from.headers", out var headers) && !string.IsNullOrWhiteSpace(headers))
        {
            _headers = headers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        _move = config.TryGetValue("header.from.operation", out var op)
            && string.Equals(op, "move", StringComparison.OrdinalIgnoreCase);
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            ProcessRecord(record);
        }

        return Task.CompletedTask;
    }

    private void ProcessRecord(SinkRecord record)
    {
        if (_fields.Length == 0 || _fields.Length != _headers.Length)
        {
            EmitRecord(GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            return;
        }

        using var doc = ParseJsonValue(record);
        if (doc is null)
        {
            EmitRecord(GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            return;
        }

        if (doc.RootElement.ValueKind != JsonValueKind.Object)
        {
            EmitRecord(GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            return;
        }

        // Start with existing headers
        var outputHeaders = ConvertHeaders(record.Headers) ?? new Dictionary<string, string>();
        var root = JsonNode.Parse(doc.RootElement.GetRawText()) as JsonObject;
        if (root is null)
        {
            EmitRecord(GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            return;
        }

        for (int i = 0; i < _fields.Length; i++)
        {
            var fieldName = _fields[i];
            var headerName = _headers[i];

            if (root[fieldName] is JsonNode fieldNode)
            {
                // Extract value as string
                var fieldValue = fieldNode.GetValueKind() == JsonValueKind.String
                    ? fieldNode.GetValue<string>()
                    : fieldNode.ToJsonString();

                outputHeaders[headerName] = fieldValue;

                if (_move)
                {
                    root.Remove(fieldName);
                }
            }
        }

        EmitRecord(GetKeyString(record), root.ToJsonString(), outputHeaders);
    }
}

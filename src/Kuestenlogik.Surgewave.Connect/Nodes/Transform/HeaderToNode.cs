namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Header-to node that copies or moves header values into JSON record fields.
/// </summary>
[ConnectorMetadata(
    Name = "HeaderTo",
    Description = "Copy or move header values into JSON record fields",
    Tags = "transform,header,field,inject")]
public sealed class HeaderToNode : ProcessorConnector
{
    public override Type TaskClass => typeof(HeaderToNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for transformed records")
        .Define("header.to.headers", ConfigType.String, "", Importance.High,
            "Comma-separated header names to read from")
        .Define("header.to.fields", ConfigType.String, "", Importance.High,
            "Comma-separated field names to inject into JSON (must match header count)")
        .Define("header.to.operation", ConfigType.String, "copy", Importance.Medium,
            "Operation: 'copy' keeps the header, 'move' removes it");
}

internal sealed class HeaderToNodeTask : ProcessorTask
{
    private string[] _headerNames = [];
    private string[] _fieldNames = [];
    private bool _move;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);

        if (config.TryGetValue("header.to.headers", out var headers) && !string.IsNullOrWhiteSpace(headers))
        {
            _headerNames = headers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        if (config.TryGetValue("header.to.fields", out var fields) && !string.IsNullOrWhiteSpace(fields))
        {
            _fieldNames = fields.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        _move = config.TryGetValue("header.to.operation", out var op)
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
        if (_headerNames.Length == 0 || _headerNames.Length != _fieldNames.Length)
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

        var root = JsonNode.Parse(doc.RootElement.GetRawText()) as JsonObject;
        if (root is null)
        {
            EmitRecord(GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            return;
        }

        // Build mutable headers for potential removal
        var outputHeaders = ConvertHeaders(record.Headers) ?? new Dictionary<string, string>();

        for (int i = 0; i < _headerNames.Length; i++)
        {
            var headerName = _headerNames[i];
            var fieldName = _fieldNames[i];

            if (outputHeaders.TryGetValue(headerName, out var headerValue))
            {
                root[fieldName] = headerValue;

                if (_move)
                {
                    outputHeaders.Remove(headerName);
                }
            }
        }

        EmitRecord(GetKeyString(record), root.ToJsonString(), outputHeaders.Count > 0 ? outputHeaders : null);
    }
}

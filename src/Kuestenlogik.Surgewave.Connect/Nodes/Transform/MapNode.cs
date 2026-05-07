namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Connect.Configuration;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

/// <summary>
/// JSON mapping node that transforms records using field mappings.
/// </summary>
[ConnectorMetadata(
    Name = "Map",
    Description = "Transform records with field mapping",
    Tags = "transform,map,projection")]
public sealed class MapNode : ProcessorConnector
{
    public override Type TaskClass => typeof(MapNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for mapped records")
        .Define("include.original", ConfigType.Boolean, "false", Importance.Medium,
            "Include all original fields");
}

internal sealed partial class MapNodeTask : ProcessorTask
{
    private readonly List<(string target, string source, bool isLiteral)> _mappings = [];
    private bool _includeOriginal;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _includeOriginal = config.TryGetValue("include.original", out var v) && bool.TryParse(v, out var b) && b;

        foreach (var (key, value) in config)
        {
            if (key.StartsWith("mapping.", StringComparison.OrdinalIgnoreCase))
            {
                var targetField = key[8..];
                var isLiteral = value.StartsWith('\'') && value.EndsWith('\'');
                var source = isLiteral ? value.Trim('\'') : value;
                _mappings.Add((targetField, source, isLiteral));
            }
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var mapped = MapRecord(record);
            if (mapped is not null)
            {
                EmitRecord(GetKeyString(record), mapped, ConvertHeaders(record.Headers));
            }
        }

        return Task.CompletedTask;
    }

    private string? MapRecord(SinkRecord record)
    {
        using var doc = ParseJsonValue(record);
        if (doc is null)
            return null;

        var result = new JsonObject();

        if (_includeOriginal && doc.RootElement.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                result[prop.Name] = JsonNode.Parse(prop.Value.GetRawText());
            }
        }

        foreach (var (target, source, isLiteral) in _mappings)
        {
            if (isLiteral)
            {
                result[target] = source;
            }
            else
            {
                var element = GetJsonPath(doc.RootElement, source);
                if (element.ValueKind != JsonValueKind.Undefined)
                {
                    result[target] = JsonNode.Parse(element.GetRawText());
                }
            }
        }

        return result.ToJsonString();
    }

    private static JsonElement GetJsonPath(JsonElement root, string path)
    {
        var current = root;
        var parts = path.TrimStart('$', '.').Split('.');

        foreach (var part in parts)
        {
            if (string.IsNullOrEmpty(part))
                continue;

            var arrayMatch = ArrayIndexPattern().Match(part);
            if (arrayMatch.Success)
            {
                var fieldName = arrayMatch.Groups["field"].Value;
                var index = int.Parse(arrayMatch.Groups["index"].Value);

                if (!string.IsNullOrEmpty(fieldName))
                {
                    if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(fieldName, out current))
                        return default;
                }

                if (current.ValueKind != JsonValueKind.Array || index >= current.GetArrayLength())
                    return default;

                current = current[index];
            }
            else
            {
                if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(part, out var next))
                    return default;

                current = next;
            }
        }

        return current;
    }

    [GeneratedRegex(@"^(?<field>\w*)?\[(?<index>\d+)\]$")]
    private static partial Regex ArrayIndexPattern();
}

namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Replace field node that filters, excludes, and renames fields in JSON records.
/// </summary>
[ConnectorMetadata(
    Name = "ReplaceField",
    Description = "Include, exclude, or rename fields in records",
    Tags = "transform,replace,field,rename,project")]
public sealed class ReplaceFieldNode : ProcessorConnector
{
    public override Type TaskClass => typeof(ReplaceFieldNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for transformed records")
        .Define("fields.include", ConfigType.String, "", Importance.Medium,
            "Comma-separated list of fields to include (allowlist)")
        .Define("fields.exclude", ConfigType.String, "", Importance.Medium,
            "Comma-separated list of fields to exclude (blocklist, takes precedence)")
        .Define("fields.renames", ConfigType.String, "", Importance.Medium,
            "Comma-separated rename pairs (old:new,foo:bar)");
}

internal sealed class ReplaceFieldNodeTask : ProcessorTask
{
    private HashSet<string>? _includeFields;
    private HashSet<string>? _excludeFields;
    private Dictionary<string, string>? _renames;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);

        if (config.TryGetValue("fields.include", out var include) && !string.IsNullOrWhiteSpace(include))
        {
            _includeFields = new HashSet<string>(
                include.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
                StringComparer.Ordinal);
        }

        if (config.TryGetValue("fields.exclude", out var exclude) && !string.IsNullOrWhiteSpace(exclude))
        {
            _excludeFields = new HashSet<string>(
                exclude.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
                StringComparer.Ordinal);
        }

        if (config.TryGetValue("fields.renames", out var renames) && !string.IsNullOrWhiteSpace(renames))
        {
            _renames = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var pair in renames.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                var parts = pair.Split(':', 2);
                if (parts.Length == 2)
                {
                    _renames[parts[0].Trim()] = parts[1].Trim();
                }
            }
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var transformed = TransformRecord(record);
            EmitRecord(GetKeyString(record), transformed, ConvertHeaders(record.Headers));
        }

        return Task.CompletedTask;
    }

    private object TransformRecord(SinkRecord record)
    {
        using var doc = ParseJsonValue(record);
        if (doc is null)
            return record.Value;

        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            return record.Value;

        var result = new JsonObject();

        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            var fieldName = prop.Name;

            // Apply include filter: if set, only keep listed fields
            if (_includeFields is not null && !_includeFields.Contains(fieldName))
                continue;

            // Apply exclude filter: takes precedence
            if (_excludeFields is not null && _excludeFields.Contains(fieldName))
                continue;

            // Apply rename
            var outputName = _renames is not null && _renames.TryGetValue(fieldName, out var renamed)
                ? renamed
                : fieldName;

            result[outputName] = JsonNode.Parse(prop.Value.GetRawText());
        }

        return result.ToJsonString();
    }
}

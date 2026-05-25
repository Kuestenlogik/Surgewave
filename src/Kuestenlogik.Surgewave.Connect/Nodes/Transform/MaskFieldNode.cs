namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Mask field node that replaces specified field values with type-aware defaults.
/// </summary>
[ConnectorMetadata(
    Name = "MaskField",
    Description = "Mask sensitive fields with type-aware default values",
    Tags = "transform,mask,field,redact,privacy")]
public sealed class MaskFieldNode : ProcessorConnector
{
    public override Type TaskClass => typeof(MaskFieldNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for masked records")
        .Define("mask.fields", ConfigType.String, "", Importance.High,
            "Comma-separated list of fields to mask (supports dot-notation for nested fields)")
        .Define("mask.replacement", ConfigType.String, "", Importance.Medium,
            "Custom replacement value (if empty, uses type-aware defaults: number->0, boolean->false, string->\"\", array->[])");
}

internal sealed class MaskFieldNodeTask : ProcessorTask
{
    private string[] _fields = [];
    private string _customReplacement = "";
    private bool _hasCustomReplacement;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);

        if (config.TryGetValue("mask.fields", out var fields) && !string.IsNullOrWhiteSpace(fields))
        {
            _fields = fields.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        _hasCustomReplacement = config.TryGetValue("mask.replacement", out var replacement) && !string.IsNullOrEmpty(replacement);
        _customReplacement = replacement ?? "";
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        foreach (var record in records)
        {
            var masked = MaskRecord(record);
            EmitRecord(GetKeyString(record), masked, ConvertHeaders(record.Headers));
        }

        return Task.CompletedTask;
    }

    private object MaskRecord(SinkRecord record)
    {
        if (_fields.Length == 0)
            return record.Value;

        using var doc = ParseJsonValue(record);
        if (doc is null)
            return record.Value;

        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            return record.Value;

        var root = JsonNode.Parse(doc.RootElement.GetRawText());
        if (root is not JsonObject rootObj)
            return record.Value;

        foreach (var fieldPath in _fields)
        {
            MaskField(rootObj, fieldPath);
        }

        return rootObj.ToJsonString();
    }

    private void MaskField(JsonObject root, string fieldPath)
    {
        var parts = fieldPath.Split('.');

        if (parts.Length == 1)
        {
            MaskDirectField(root, parts[0]);
            return;
        }

        // Navigate to the parent of the target field
        JsonObject current = root;
        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (current[parts[i]] is JsonObject nested)
            {
                current = nested;
            }
            else
            {
                return; // Path not found
            }
        }

        MaskDirectField(current, parts[^1]);
    }

    private void MaskDirectField(JsonObject obj, string fieldName)
    {
        if (!obj.ContainsKey(fieldName))
            return;

        var existing = obj[fieldName];

        if (_hasCustomReplacement)
        {
            obj[fieldName] = JsonValue.Create(_customReplacement);
            return;
        }

        // Type-aware masking
        obj[fieldName] = GetTypeAwareDefault(existing);
    }

    private static JsonNode? GetTypeAwareDefault(JsonNode? node)
    {
        if (node is null)
            return null;

        var element = node.GetValueKind();
        return element switch
        {
            JsonValueKind.Number => JsonValue.Create(0),
            JsonValueKind.True or JsonValueKind.False => JsonValue.Create(false),
            JsonValueKind.Array => new JsonArray(),
            JsonValueKind.Object => new JsonObject(),
            _ => JsonValue.Create("")
        };
    }
}

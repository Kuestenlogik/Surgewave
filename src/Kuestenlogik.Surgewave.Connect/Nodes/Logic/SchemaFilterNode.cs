namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Connect.Configuration;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

/// <summary>
/// Combines schema decoding with condition-based filtering.
/// For JSON payloads, applies ConditionEvaluator on the decoded content.
/// For non-JSON, can filter by schema metadata.
/// </summary>
[ConnectorMetadata(
    Name = "Schema Filter",
    Description = "Filters schema-encoded records by decoded content or schema metadata",
    Tags = "logic,schema,filter,decode,registry")]
public sealed class SchemaFilterNode : ProcessorConnector
{
    public override Type TaskClass => typeof(SchemaFilterNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for matching records")
        .Define("condition", ConfigType.String, "", Importance.High,
            "Filter condition expression (e.g., $.field == 'value')")
        .Define("negate", ConfigType.Boolean, "false", Importance.Low,
            "Negate the condition result")
        .Define("filter.mode", ConfigType.String, "content", Importance.Medium,
            "Filter mode: 'content' (filter on decoded payload) or 'metadata' (filter on schema metadata)")
        .Define("passthrough.non.encoded", ConfigType.Boolean, "true", Importance.Medium,
            "Pass through non-encoded records (filtered as plain JSON in content mode)");
}

internal sealed class SchemaFilterNodeTask : ProcessorTask
{
    private readonly ConcurrentDictionary<int, SchemaInfo> _schemaCache = new();
    private string _condition = "";
    private bool _negate;
    private string _filterMode = "content";
    private bool _passthroughNonEncoded;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _condition = config.TryGetValue("condition", out var c) ? c : "";
        _negate = config.TryGetValue("negate", out var n) && bool.TryParse(n, out var b) && b;
        _filterMode = config.TryGetValue("filter.mode", out var fm) ? fm : "content";
        _passthroughNonEncoded = !config.TryGetValue("passthrough.non.encoded", out var pt)
            || !bool.TryParse(pt, out var ptb) || ptb; // default true
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return;

        foreach (var record in records)
        {
            if (record.Value is null || record.Value.Length == 0)
                continue;

            if (WireFormatHelper.IsWireFormat(record.Value))
            {
                await ProcessEncodedRecordAsync(record, cancellationToken);
            }
            else if (_passthroughNonEncoded)
            {
                ProcessPlainRecord(record);
            }
        }
    }

    private async Task ProcessEncodedRecordAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var schemaId = WireFormatHelper.ReadSchemaId(record.Value);
        var payload = WireFormatHelper.GetPayload(record.Value);
        var schemaInfo = await GetSchemaInfoAsync(schemaId, cancellationToken);

        bool matches;
        if (_filterMode == "metadata")
        {
            matches = EvaluateMetadataCondition(schemaId, schemaInfo);
        }
        else
        {
            matches = EvaluateContentCondition(payload);
        }

        var shouldPass = _negate ? !matches : matches;
        if (!shouldPass)
            return;

        var headers = ConvertHeaders(record.Headers) ?? [];
        headers["_schema.id"] = schemaId.ToString();
        if (schemaInfo is not null)
        {
            headers["_schema.type"] = schemaInfo.SchemaType;
            headers["_schema.subject"] = schemaInfo.Subject;
            headers["_schema.version"] = schemaInfo.Version.ToString();
        }

        EmitRecord(GetKeyString(record), payload, headers);
    }

    private void ProcessPlainRecord(SinkRecord record)
    {
        if (_filterMode == "metadata")
            return; // Non-encoded records have no schema metadata

        var matches = EvaluateContentCondition(record.Value);
        var shouldPass = _negate ? !matches : matches;

        if (shouldPass)
        {
            EmitRecord(GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
        }
    }

    private bool EvaluateContentCondition(byte[] payload)
    {
        if (string.IsNullOrEmpty(_condition))
            return true;

        try
        {
            using var doc = JsonDocument.Parse(payload);
            return ConditionEvaluator.Evaluate(_condition, doc.RootElement);
        }
        catch
        {
            return false;
        }
    }

    private bool EvaluateMetadataCondition(int schemaId, SchemaInfo? schemaInfo)
    {
        if (string.IsNullOrEmpty(_condition))
            return true;

        var metadataJson = BuildMetadataJson(schemaId, schemaInfo);
        try
        {
            using var doc = JsonDocument.Parse(metadataJson);
            return ConditionEvaluator.Evaluate(_condition, doc.RootElement);
        }
        catch
        {
            return false;
        }
    }

    private static string BuildMetadataJson(int schemaId, SchemaInfo? schemaInfo)
    {
        if (schemaInfo is null)
        {
            return $$$"""{"schema":{"id":{{{schemaId}}}}}""";
        }

        return $$$"""{"schema":{"id":{{{schemaInfo.Id}}},"type":"{{{schemaInfo.SchemaType}}}","subject":"{{{schemaInfo.Subject}}}","version":{{{schemaInfo.Version}}}}}""";
    }

    private async Task<SchemaInfo?> GetSchemaInfoAsync(int schemaId, CancellationToken cancellationToken)
    {
        if (_schemaCache.TryGetValue(schemaId, out var cached))
            return cached;

        if (SchemaRegistry is null)
            return null;

        var info = await SchemaRegistry.GetSchemaByIdAsync(schemaId, cancellationToken);
        if (info is not null)
        {
            _schemaCache.TryAdd(schemaId, info);
        }

        return info;
    }
}

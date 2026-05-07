namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Connect.Configuration;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

/// <summary>
/// Debug/inspection node that emits records with full metadata as structured JSON.
/// Auto-detects wire format and decodes schema information if available.
/// </summary>
[ConnectorMetadata(
    Name = "Message Inspector",
    Description = "Inspects and enriches records with full metadata for debugging and display",
    Tags = "logic,inspect,debug,display,message")]
public sealed class MessageInspectorNode : ProcessorConnector
{
    public override Type TaskClass => typeof(MessageInspectorNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for inspected records")
        .Define("output.format", ConfigType.String, "full", Importance.Medium,
            "Output format: 'full', 'compact', or 'headers-only'")
        .Define("decode.schema", ConfigType.Boolean, "true", Importance.Medium,
            "Auto-decode wire format encoded records")
        .Define("value.display", ConfigType.String, "auto", Importance.Low,
            "Value display mode: 'auto', 'string', 'hex', or 'base64'");
}

internal sealed class MessageInspectorNodeTask : ProcessorTask
{
    private readonly ConcurrentDictionary<int, SchemaInfo> _schemaCache = new();
    private string _outputFormat = "full";
    private bool _decodeSchema;
    private string _valueDisplay = "auto";

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _outputFormat = config.TryGetValue("output.format", out var fmt) ? fmt : "full";
        _decodeSchema = !config.TryGetValue("decode.schema", out var ds)
            || !bool.TryParse(ds, out var dsb) || dsb; // default true
        _valueDisplay = config.TryGetValue("value.display", out var vd) ? vd : "auto";
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return;

        foreach (var record in records)
        {
            var json = await BuildInspectionJsonAsync(record, cancellationToken);
            EmitRecord(GetKeyString(record), json);
        }
    }

    private async Task<string> BuildInspectionJsonAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        writer.WriteStartObject();

        var keyString = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null;

        // Decode schema if applicable
        SchemaInfo? schemaInfo = null;
        int? schemaId = null;
        byte[]? decodedPayload = null;

        if (_decodeSchema && record.Value != null && WireFormatHelper.IsWireFormat(record.Value))
        {
            schemaId = WireFormatHelper.ReadSchemaId(record.Value);
            decodedPayload = WireFormatHelper.GetPayload(record.Value);
            schemaInfo = await GetSchemaInfoAsync(schemaId.Value, cancellationToken);
        }

        switch (_outputFormat)
        {
            case "compact":
                WriteCompactFormat(writer, record, keyString, decodedPayload, schemaInfo);
                break;
            case "headers-only":
                WriteHeadersOnlyFormat(writer, record, schemaId, schemaInfo);
                break;
            default: // "full"
                WriteFullFormat(writer, record, keyString, decodedPayload, schemaId, schemaInfo);
                break;
        }

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private void WriteFullFormat(Utf8JsonWriter writer, SinkRecord record, string? keyString,
        byte[]? decodedPayload, int? schemaId, SchemaInfo? schemaInfo)
    {
        // Key
        if (keyString is not null)
            writer.WriteString("key", keyString);
        else
            writer.WriteNull("key");

        // Value
        var displayValue = decodedPayload ?? record.Value;
        writer.WriteString("value", FormatValue(displayValue));

        // Sizes
        writer.WriteNumber("valueSize", record.Value?.Length ?? 0);
        writer.WriteNumber("keySize", record.Key?.Length ?? 0);

        // Source metadata
        writer.WriteString("topic", record.Topic);
        writer.WriteNumber("partition", record.Partition);
        writer.WriteNumber("offset", record.Offset);
        writer.WriteString("timestamp", record.Timestamp.ToString("O"));

        // Headers
        WriteHeaders(writer, record.Headers);

        // Schema info
        if (schemaId.HasValue)
        {
            WriteSchemaInfo(writer, schemaId.Value, schemaInfo);

            if (decodedPayload is not null)
            {
                writer.WriteString("decodedValue", FormatValue(decodedPayload));
            }
        }
    }

    private void WriteCompactFormat(Utf8JsonWriter writer, SinkRecord record, string? keyString,
        byte[]? decodedPayload, SchemaInfo? schemaInfo)
    {
        // Key
        if (keyString is not null)
            writer.WriteString("key", keyString);
        else
            writer.WriteNull("key");

        // Value (truncated to 200 chars)
        var displayValue = decodedPayload ?? record.Value;
        var valueStr = FormatValue(displayValue);
        if (valueStr.Length > 200)
            valueStr = valueStr[..200] + "...";
        writer.WriteString("value", valueStr);

        // Topic
        writer.WriteString("topic", record.Topic);

        // Schema type if available
        if (schemaInfo is not null)
        {
            writer.WriteString("schemaType", schemaInfo.SchemaType);
        }
    }

    private static void WriteHeadersOnlyFormat(Utf8JsonWriter writer, SinkRecord record,
        int? schemaId, SchemaInfo? schemaInfo)
    {
        WriteHeaders(writer, record.Headers);

        if (schemaId.HasValue)
        {
            WriteSchemaInfo(writer, schemaId.Value, schemaInfo);
        }
    }

    private string FormatValue(byte[]? value)
    {
        if (value is null || value.Length == 0)
            return "";

        return _valueDisplay switch
        {
            "hex" => Convert.ToHexString(value),
            "base64" => Convert.ToBase64String(value),
            "string" => Encoding.UTF8.GetString(value),
            _ => IsLikelyUtf8(value) ? Encoding.UTF8.GetString(value) : Convert.ToBase64String(value) // auto
        };
    }

    private static bool IsLikelyUtf8(byte[] data)
    {
        try
        {
            _ = Encoding.UTF8.GetString(data);
            // Check if it contains non-printable control characters (excluding common whitespace)
            foreach (var b in data)
            {
                if (b < 0x20 && b != 0x09 && b != 0x0A && b != 0x0D) // tab, newline, carriage return
                    return false;
            }
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static void WriteHeaders(Utf8JsonWriter writer, IReadOnlyDictionary<string, byte[]>? headers)
    {
        writer.WriteStartObject("headers");
        if (headers is not null)
        {
            foreach (var (key, value) in headers)
            {
                writer.WriteString(key, Encoding.UTF8.GetString(value));
            }
        }
        writer.WriteEndObject();
    }

    private static void WriteSchemaInfo(Utf8JsonWriter writer, int schemaId, SchemaInfo? schemaInfo)
    {
        writer.WriteStartObject("schema");
        writer.WriteNumber("id", schemaId);
        if (schemaInfo is not null)
        {
            writer.WriteString("type", schemaInfo.SchemaType);
            writer.WriteString("subject", schemaInfo.Subject);
            writer.WriteNumber("version", schemaInfo.Version);
        }
        writer.WriteEndObject();
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

namespace Kuestenlogik.Surgewave.Connect.Nodes.Transform;

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Decodes schema-registry wire-format encoded records.
/// Strips the 5-byte header [0x00][4-byte Schema-ID], resolves schema metadata,
/// and emits the payload with schema information as headers.
/// </summary>
[ConnectorMetadata(
    Name = "Schema Decode",
    Description = "Decodes schema-registry-encoded records and strips wire format header",
    Tags = "transform,schema,decode,registry")]
public sealed class SchemaDecodeNode : ProcessorConnector
{
    public override Type TaskClass => typeof(SchemaDecodeNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for decoded records")
        .Define("include.schema.string", ConfigType.Boolean, "false", Importance.Low,
            "Include full schema definition as _schema.string header")
        .Define("passthrough.non.encoded", ConfigType.Boolean, "true", Importance.Medium,
            "Pass through records without wire format header unchanged");
}

internal sealed class SchemaDecodeNodeTask : ProcessorTask
{
    private readonly ConcurrentDictionary<int, SchemaInfo> _schemaCache = new();
    private bool _includeSchemaString;
    private bool _passthroughNonEncoded;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _includeSchemaString = config.TryGetValue("include.schema.string", out var inc)
            && bool.TryParse(inc, out var b) && b;
        _passthroughNonEncoded = !config.TryGetValue("passthrough.non.encoded", out var pt)
            || !bool.TryParse(pt, out var ptb) || ptb; // default true
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
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
                EmitRecord(GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            }
        }
    }

    private async Task ProcessEncodedRecordAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var schemaId = WireFormatHelper.ReadSchemaId(record.Value);
        var payload = WireFormatHelper.GetPayload(record.Value);

        var headers = ConvertHeaders(record.Headers) ?? [];
        headers["_schema.id"] = schemaId.ToString();

        var schemaInfo = await GetSchemaInfoAsync(schemaId, cancellationToken);
        if (schemaInfo is not null)
        {
            headers["_schema.type"] = schemaInfo.SchemaType;
            headers["_schema.subject"] = schemaInfo.Subject;
            headers["_schema.version"] = schemaInfo.Version.ToString();

            if (_includeSchemaString)
            {
                headers["_schema.string"] = schemaInfo.SchemaString;
            }
        }

        EmitRecord(GetKeyString(record), payload, headers);
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

/// <summary>
/// Helper for reading Confluent-compatible wire format: [0x00][4-byte big-endian Schema-ID][Payload].
/// </summary>
internal static class WireFormatHelper
{
    public const int HeaderSize = 5;

    public static bool IsWireFormat(byte[] data)
    {
        return data.Length >= HeaderSize && data[0] == 0x00;
    }

    public static int ReadSchemaId(byte[] data)
    {
        return BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(1, 4));
    }

    public static byte[] GetPayload(byte[] data)
    {
        return data[HeaderSize..];
    }
}

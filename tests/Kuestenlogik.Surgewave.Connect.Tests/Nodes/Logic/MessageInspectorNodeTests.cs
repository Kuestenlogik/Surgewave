namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Logic;

using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Connect.Nodes;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;
using Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

public class MessageInspectorNodeTests
{
    private static readonly SchemaInfo TestSchema = new()
    {
        Id = 42,
        Subject = "users-value",
        Version = 3,
        SchemaType = "JSON",
        SchemaString = """{"type":"object"}"""
    };

    private static byte[] CreateWireFormatPayload(int schemaId, string jsonPayload)
    {
        var payload = Encoding.UTF8.GetBytes(jsonPayload);
        var result = new byte[5 + payload.Length];
        result[0] = 0x00;
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(1, 4), schemaId);
        payload.CopyTo(result, 5);
        return result;
    }

    private static SinkRecord CreateRecord(
        byte[] value,
        string? key = null,
        string topic = "users",
        int partition = 2,
        long offset = 1542,
        DateTimeOffset? timestamp = null,
        Dictionary<string, byte[]>? headers = null)
    {
        return new SinkRecord
        {
            Topic = topic,
            Partition = partition,
            Offset = offset,
            Timestamp = timestamp ?? new DateTimeOffset(2026, 2, 27, 10, 30, 0, TimeSpan.Zero),
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = value,
            Headers = headers
        };
    }

    private static SinkRecord CreateRecord(
        string value,
        string? key = null,
        string topic = "users",
        int partition = 2,
        long offset = 1542,
        DateTimeOffset? timestamp = null,
        Dictionary<string, byte[]>? headers = null)
    {
        return CreateRecord(Encoding.UTF8.GetBytes(value), key, topic, partition, offset, timestamp, headers);
    }

    private static MessageInspectorNodeTask CreateTask(
        ISchemaRegistryOperations? registry = null,
        string outputFormat = "full",
        bool decodeSchema = true,
        string valueDisplay = "auto")
    {
        var task = new MessageInspectorNodeTask();
        task.Initialize(new TaskContext { SchemaRegistry = registry });
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["output.format"] = outputFormat,
            ["decode.schema"] = decodeSchema.ToString().ToLowerInvariant(),
            ["value.display"] = valueDisplay
        });
        return task;
    }

    private static JsonDocument GetEmittedJson(ProcessorTask task, int index = 0)
    {
        var json = Encoding.UTF8.GetString(task.EmittedRecords[index].Value);
        return JsonDocument.Parse(json);
    }

    [Fact]
    public async Task FullFormat_AllMetadataIncluded()
    {
        var task = CreateTask();
        var headers = new Dictionary<string, byte[]>
        {
            ["x-trace"] = Encoding.UTF8.GetBytes("abc"),
            ["x-source"] = Encoding.UTF8.GetBytes("api")
        };
        var record = CreateRecord("""{"name":"Alice","age":30}""", "user-123", headers: headers);

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = GetEmittedJson(task);
        var root = doc.RootElement;

        Assert.Equal("user-123", root.GetProperty("key").GetString());
        Assert.Contains("Alice", root.GetProperty("value").GetString());
        Assert.Equal("users", root.GetProperty("topic").GetString());
        Assert.Equal(2, root.GetProperty("partition").GetInt32());
        Assert.Equal(1542, root.GetProperty("offset").GetInt64());
        Assert.True(root.TryGetProperty("timestamp", out _));
        Assert.Equal(8, root.GetProperty("keySize").GetInt32());
        Assert.True(root.GetProperty("valueSize").GetInt32() > 0);
        Assert.Equal("abc", root.GetProperty("headers").GetProperty("x-trace").GetString());
        Assert.Equal("api", root.GetProperty("headers").GetProperty("x-source").GetString());
    }

    [Fact]
    public async Task CompactFormat_TruncatedValue()
    {
        var task = CreateTask(outputFormat: "compact");
        var longValue = "{\"data\":\"" + new string('X', 300) + "\"}";
        var record = CreateRecord(longValue, "k1");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = GetEmittedJson(task);
        var root = doc.RootElement;

        Assert.Equal("k1", root.GetProperty("key").GetString());
        Assert.Equal("users", root.GetProperty("topic").GetString());
        var valueStr = root.GetProperty("value").GetString()!;
        Assert.True(valueStr.Length <= 203); // 200 + "..."
        Assert.EndsWith("...", valueStr);
    }

    [Fact]
    public async Task HeadersOnlyFormat_NoValueInOutput()
    {
        var task = CreateTask(outputFormat: "headers-only");
        var headers = new Dictionary<string, byte[]>
        {
            ["x-id"] = Encoding.UTF8.GetBytes("123")
        };
        var record = CreateRecord("""{"data":1}""", headers: headers);

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = GetEmittedJson(task);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("headers", out var hdr));
        Assert.Equal("123", hdr.GetProperty("x-id").GetString());
        Assert.False(root.TryGetProperty("value", out _));
        Assert.False(root.TryGetProperty("key", out _));
    }

    [Fact]
    public async Task SchemaDecoded_WireFormat_IncludesSchemaInfo()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry);
        var wireData = CreateWireFormatPayload(42, """{"name":"Alice"}""");
        var record = CreateRecord(wireData, "user-123");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = GetEmittedJson(task);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("schema", out var schema));
        Assert.Equal(42, schema.GetProperty("id").GetInt32());
        Assert.Equal("JSON", schema.GetProperty("type").GetString());
        Assert.Equal("users-value", schema.GetProperty("subject").GetString());
        Assert.Equal(3, schema.GetProperty("version").GetInt32());

        Assert.True(root.TryGetProperty("decodedValue", out var decoded));
        Assert.Contains("Alice", decoded.GetString());
    }

    [Fact]
    public async Task NonEncoded_NoSchemaSection()
    {
        var task = CreateTask();
        var record = CreateRecord("""{"plain":"data"}""", "k1");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = GetEmittedJson(task);
        var root = doc.RootElement;

        Assert.False(root.TryGetProperty("schema", out _));
        Assert.False(root.TryGetProperty("decodedValue", out _));
    }

    [Fact]
    public async Task ValueDisplay_Hex_BinaryOutput()
    {
        var task = CreateTask(valueDisplay: "hex");
        var binaryValue = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var record = CreateRecord(binaryValue, "k1");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = GetEmittedJson(task);
        Assert.Equal("DEADBEEF", doc.RootElement.GetProperty("value").GetString());
    }

    [Fact]
    public async Task ValueDisplay_Base64_BinaryOutput()
    {
        var task = CreateTask(valueDisplay: "base64");
        var binaryValue = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var record = CreateRecord(binaryValue, "k1");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = GetEmittedJson(task);
        Assert.Equal(Convert.ToBase64String(binaryValue), doc.RootElement.GetProperty("value").GetString());
    }

    [Fact]
    public async Task NullKey_HandledGracefully()
    {
        var task = CreateTask();
        var record = CreateRecord("""{"data":1}""", key: null);

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = GetEmittedJson(task);
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("key").ValueKind);
    }

    [Fact]
    public async Task EmptyHeaders_EmptyObject()
    {
        var task = CreateTask();
        var record = CreateRecord("""{"data":1}""");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = GetEmittedJson(task);
        var headers = doc.RootElement.GetProperty("headers");
        Assert.Equal(JsonValueKind.Object, headers.ValueKind);
        Assert.False(headers.EnumerateObject().Any());
    }

    [Fact]
    public async Task NoOutputTopic_NoEmit()
    {
        var task = new MessageInspectorNodeTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            ["output.format"] = "full"
        });

        var record = CreateRecord("""{"data":1}""");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }
}

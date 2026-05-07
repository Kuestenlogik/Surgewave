namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Connect.Nodes;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class SchemaDecodeNodeTests
{
    private static readonly SchemaInfo TestSchema = new()
    {
        Id = 42,
        Subject = "test-value",
        Version = 1,
        SchemaType = "JSON",
        SchemaString = """{"type":"object","properties":{"name":{"type":"string"}}}"""
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

    private static SinkRecord CreateRecord(byte[] value, string? key = null, Dictionary<string, byte[]>? headers = null)
    {
        return new SinkRecord
        {
            Topic = "input",
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = value,
            Headers = headers
        };
    }

    private static SinkRecord CreateRecord(string value, string? key = null, Dictionary<string, byte[]>? headers = null)
    {
        return CreateRecord(Encoding.UTF8.GetBytes(value), key, headers);
    }

    private static SchemaDecodeNodeTask CreateTask(
        ISchemaRegistryOperations? registry = null,
        bool includeSchemaString = false,
        bool passthroughNonEncoded = true)
    {
        var task = new SchemaDecodeNodeTask();
        task.Initialize(new TaskContext { SchemaRegistry = registry });
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["include.schema.string"] = includeSchemaString.ToString().ToLowerInvariant(),
            ["passthrough.non.encoded"] = passthroughNonEncoded.ToString().ToLowerInvariant()
        });
        return task;
    }

    private static string GetEmittedValue(ProcessorTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    private static string? GetEmittedHeader(ProcessorTask task, string headerName, int index = 0)
    {
        var headers = task.EmittedRecords[index].Headers;
        return headers != null && headers.TryGetValue(headerName, out var val)
            ? Encoding.UTF8.GetString(val) : null;
    }

    [Fact]
    public async Task WireFormat_JsonPayload_StripsHeaderAndEmitsJson()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry);
        var wireData = CreateWireFormatPayload(42, """{"name":"Alice"}""");
        var record = CreateRecord(wireData, "key-1");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("""{"name":"Alice"}""", GetEmittedValue(task));
        Assert.Equal("42", GetEmittedHeader(task, "_schema.id"));
        Assert.Equal("JSON", GetEmittedHeader(task, "_schema.type"));
    }

    [Fact]
    public async Task WireFormat_BinaryPayload_EmitsWithSchemaHeaders()
    {
        var avroSchema = TestSchema with { Id = 10, SchemaType = "AVRO" };
        var registry = new MockSchemaRegistry();
        registry.AddSchema(10, avroSchema);

        var task = CreateTask(registry);
        var binaryPayload = new byte[] { 0x01, 0x02, 0x03 };
        var wireData = new byte[5 + binaryPayload.Length];
        wireData[0] = 0x00;
        BinaryPrimitives.WriteInt32BigEndian(wireData.AsSpan(1, 4), 10);
        binaryPayload.CopyTo(wireData, 5);

        var record = CreateRecord(wireData);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal(binaryPayload, task.EmittedRecords[0].Value);
        Assert.Equal("AVRO", GetEmittedHeader(task, "_schema.type"));
        Assert.Equal("test-value", GetEmittedHeader(task, "_schema.subject"));
        Assert.Equal("1", GetEmittedHeader(task, "_schema.version"));
    }

    [Fact]
    public async Task NonEncoded_Passthrough_True_ForwardsRecord()
    {
        var task = CreateTask(passthroughNonEncoded: true);
        var record = CreateRecord("""{"plain":"json"}""", "k1");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("""{"plain":"json"}""", GetEmittedValue(task));
    }

    [Fact]
    public async Task NonEncoded_Passthrough_False_DropsRecord()
    {
        var task = CreateTask(passthroughNonEncoded: false);
        var record = CreateRecord("""{"plain":"json"}""");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task SchemaCache_SecondLookupUsesCache()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry);
        var wire1 = CreateWireFormatPayload(42, """{"a":1}""");
        var wire2 = CreateWireFormatPayload(42, """{"b":2}""");

        await task.PutAsync([CreateRecord(wire1), CreateRecord(wire2)], CancellationToken.None);

        Assert.Equal(2, task.EmittedRecords.Count);
        Assert.Equal(1, registry.GetLookupCount(42));
    }

    [Fact]
    public async Task IncludeSchemaString_AddsSchemaDefinitionHeader()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry, includeSchemaString: true);
        var wireData = CreateWireFormatPayload(42, """{"x":1}""");

        await task.PutAsync([CreateRecord(wireData)], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal(TestSchema.SchemaString, GetEmittedHeader(task, "_schema.string"));
    }

    [Fact]
    public async Task NoSchemaRegistry_PassthroughAll()
    {
        var task = CreateTask(registry: null, passthroughNonEncoded: true);

        var wireData = CreateWireFormatPayload(42, """{"encoded":true}""");
        var plain = CreateRecord("""{"plain":true}""");

        await task.PutAsync([CreateRecord(wireData), plain], CancellationToken.None);

        // Wire-format record is still decoded (header stripped), just no schema metadata beyond ID
        Assert.Equal(2, task.EmittedRecords.Count);
        Assert.Equal("42", GetEmittedHeader(task, "_schema.id", 0));
        Assert.Null(GetEmittedHeader(task, "_schema.type", 0));
    }

    [Fact]
    public async Task EmptyValue_SkipsRecord()
    {
        var task = CreateTask();
        var record = CreateRecord(Array.Empty<byte>());

        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task NullValue_SkipsRecord()
    {
        var task = CreateTask();
        var record = new SinkRecord
        {
            Topic = "input",
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = null,
            Value = null!,
            Headers = null
        };

        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task BatchMixed_EncodedAndPlain()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry, passthroughNonEncoded: true);

        var encoded = CreateRecord(CreateWireFormatPayload(42, """{"type":"encoded"}"""), "k1");
        var plain = CreateRecord("""{"type":"plain"}""", "k2");
        var encoded2 = CreateRecord(CreateWireFormatPayload(42, """{"type":"encoded2"}"""), "k3");

        await task.PutAsync([encoded, plain, encoded2], CancellationToken.None);

        Assert.Equal(3, task.EmittedRecords.Count);

        // First: encoded - has schema headers
        Assert.Equal("""{"type":"encoded"}""", GetEmittedValue(task, 0));
        Assert.Equal("JSON", GetEmittedHeader(task, "_schema.type", 0));

        // Second: plain - no schema headers
        Assert.Equal("""{"type":"plain"}""", GetEmittedValue(task, 1));
        Assert.Null(GetEmittedHeader(task, "_schema.type", 1));

        // Third: encoded - has schema headers
        Assert.Equal("""{"type":"encoded2"}""", GetEmittedValue(task, 2));
        Assert.Equal("42", GetEmittedHeader(task, "_schema.id", 2));
    }
}

/// <summary>
/// Simple mock for ISchemaRegistryOperations used across schema-related node tests.
/// </summary>
internal sealed class MockSchemaRegistry : ISchemaRegistryOperations
{
    private readonly Dictionary<int, SchemaInfo> _schemas = [];
    private readonly Dictionary<int, int> _lookupCounts = [];

    public void AddSchema(int id, SchemaInfo info) => _schemas[id] = info;

    public int GetLookupCount(int id) => _lookupCounts.GetValueOrDefault(id, 0);

    public Task<SchemaInfo?> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default)
    {
        _lookupCounts[schemaId] = _lookupCounts.GetValueOrDefault(schemaId, 0) + 1;
        return Task.FromResult(_schemas.GetValueOrDefault(schemaId));
    }

    public Task<SchemaRegistrationResult> RegisterSchemaAsync(string subject, string schemaString, string schemaType, CancellationToken cancellationToken = default)
        => throw new NotImplementedException();

    public Task<SchemaInfo?> GetSchemaByVersionAsync(string subject, int version, CancellationToken cancellationToken = default)
        => throw new NotImplementedException();

    public Task<IReadOnlyList<int>> GetSubjectVersionsAsync(string subject, CancellationToken cancellationToken = default)
        => throw new NotImplementedException();
}

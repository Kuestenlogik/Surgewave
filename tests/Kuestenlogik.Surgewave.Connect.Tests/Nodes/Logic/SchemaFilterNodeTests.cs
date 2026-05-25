namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Logic;

using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Connect.Nodes;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;
using Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

public class SchemaFilterNodeTests
{
    private static readonly SchemaInfo TestSchema = new()
    {
        Id = 42,
        Subject = "test-value",
        Version = 1,
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

    private static SinkRecord CreateRecord(byte[] value, string? key = null)
    {
        return new SinkRecord
        {
            Topic = "input",
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = value,
            Headers = null
        };
    }

    private static SinkRecord CreateRecord(string value, string? key = null)
    {
        return CreateRecord(Encoding.UTF8.GetBytes(value), key);
    }

    private static SchemaFilterNodeTask CreateTask(
        ISchemaRegistryOperations? registry = null,
        string condition = "",
        bool negate = false,
        string filterMode = "content",
        bool passthroughNonEncoded = true)
    {
        var task = new SchemaFilterNodeTask();
        task.Initialize(new TaskContext { SchemaRegistry = registry });
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["condition"] = condition,
            ["negate"] = negate.ToString().ToLowerInvariant(),
            ["filter.mode"] = filterMode,
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
    public async Task ContentFilter_JsonPayload_MatchesCondition()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry, condition: "$.status == 'active'");
        var wire = CreateWireFormatPayload(42, """{"status":"active","name":"Alice"}""");

        await task.PutAsync([CreateRecord(wire)], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("""{"status":"active","name":"Alice"}""", GetEmittedValue(task));
        Assert.Equal("JSON", GetEmittedHeader(task, "_schema.type"));
    }

    [Fact]
    public async Task ContentFilter_JsonPayload_NoMatch_Filtered()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry, condition: "$.status == 'active'");
        var wire = CreateWireFormatPayload(42, """{"status":"inactive"}""");

        await task.PutAsync([CreateRecord(wire)], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task ContentFilter_Negate_InvertsResult()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry, condition: "$.status == 'active'", negate: true);
        var wire = CreateWireFormatPayload(42, """{"status":"active"}""");

        await task.PutAsync([CreateRecord(wire)], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task MetadataFilter_BySchemaType()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry, condition: "$.schema.type == 'JSON'", filterMode: "metadata");
        var wire = CreateWireFormatPayload(42, """{"data":1}""");

        await task.PutAsync([CreateRecord(wire)], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
    }

    [Fact]
    public async Task MetadataFilter_BySubject()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry, condition: "$.schema.subject == 'other-subject'", filterMode: "metadata");
        var wire = CreateWireFormatPayload(42, """{"data":1}""");

        await task.PutAsync([CreateRecord(wire)], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task NonEncoded_Passthrough_FilteredAsJson()
    {
        var task = CreateTask(condition: "$.level == 'error'");
        var record = CreateRecord("""{"level":"error","msg":"oops"}""");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("""{"level":"error","msg":"oops"}""", GetEmittedValue(task));
    }

    [Fact]
    public async Task NonEncoded_Passthrough_False_DropsNonEncoded()
    {
        var task = CreateTask(condition: "$.level == 'error'", passthroughNonEncoded: false);
        var record = CreateRecord("""{"level":"error"}""");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task NoCondition_PassesAll()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry, condition: "");
        var wire = CreateWireFormatPayload(42, """{"any":"data"}""");

        await task.PutAsync([CreateRecord(wire)], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
    }

    [Fact]
    public async Task NoSchemaRegistry_PlainJsonFilter()
    {
        var task = CreateTask(registry: null, condition: "$.x > 5");
        var record = CreateRecord("""{"x":10}""");

        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
    }

    [Fact]
    public async Task BatchMixed_SomeMatchSomeDont()
    {
        var registry = new MockSchemaRegistry();
        registry.AddSchema(42, TestSchema);

        var task = CreateTask(registry, condition: "$.active == 'true'", passthroughNonEncoded: true);

        var match = CreateRecord(CreateWireFormatPayload(42, """{"active":"true"}"""), "k1");
        var noMatch = CreateRecord(CreateWireFormatPayload(42, """{"active":"false"}"""), "k2");
        var plainMatch = CreateRecord("""{"active":"true"}""", "k3");
        var plainNoMatch = CreateRecord("""{"active":"false"}""", "k4");

        await task.PutAsync([match, noMatch, plainMatch, plainNoMatch], CancellationToken.None);

        Assert.Equal(2, task.EmittedRecords.Count);
    }
}

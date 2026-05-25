namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Logic;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

public class DlqSinkNodeTests
{
    private static SinkRecord CreateErrorRecord(string errorJson, string? key = null, Dictionary<string, byte[]>? headers = null)
    {
        return new SinkRecord
        {
            Topic = "errors",
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = Encoding.UTF8.GetBytes(errorJson),
            Headers = headers
        };
    }

    [Fact]
    public async Task PutAsync_PassesThrough()
    {
        var task = new DlqSinkNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "dlq-output"
        });

        var errorPayload = """{"error":"test","nodeId":"n1"}""";
        var record = CreateErrorRecord(errorPayload, key: "key1");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var emitted = task.EmittedRecords[0];
        Assert.Equal("dlq-output", emitted.Topic);
        Assert.Equal(errorPayload, Encoding.UTF8.GetString(emitted.Value));
    }

    [Fact]
    public async Task PutAsync_PreservesHeaders()
    {
        var task = new DlqSinkNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "dlq-output"
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["_error_node_id"] = Encoding.UTF8.GetBytes("node-1"),
            ["_error_type"] = Encoding.UTF8.GetBytes("JsonException"),
            ["_error_message"] = Encoding.UTF8.GetBytes("Invalid JSON")
        };

        var record = CreateErrorRecord("""{"error":"test"}""", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var emitted = task.EmittedRecords[0];
        Assert.NotNull(emitted.Headers);
        Assert.Equal("node-1", Encoding.UTF8.GetString(emitted.Headers["_error_node_id"]));
    }

    [Fact]
    public async Task PutAsync_TruncatesLargeValues()
    {
        var task = new DlqSinkNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "dlq-output",
            ["max.value.bytes"] = "50"
        });

        var largePayload = new string('x', 200);
        var record = CreateErrorRecord(largePayload);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal(50, task.EmittedRecords[0].Value.Length);
    }
}

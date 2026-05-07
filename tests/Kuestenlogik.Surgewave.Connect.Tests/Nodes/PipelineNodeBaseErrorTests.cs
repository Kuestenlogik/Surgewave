namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes;

public class PipelineNodeBaseErrorTests
{
    /// <summary>
    /// Concrete test implementation of ProcessorTask for testing error handling.
    /// </summary>
    private sealed class TestNodeTask : ProcessorTask
    {
        public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
        {
            foreach (var record in records)
            {
                try
                {
                    var doc = ParseJsonValueOrError(record);
                    if (doc != null)
                    {
                        EmitRecord(GetKeyString(record), record.Value);
                        doc.Dispose();
                    }
                }
                catch (Exception ex)
                {
                    EmitError(record, ex);
                }
            }
            return Task.CompletedTask;
        }

        public void TestEmitError(SinkRecord record, Exception ex)
        {
            EmitError(record, ex);
        }

        public JsonDocument? TestParseJsonValueOrError(SinkRecord record)
        {
            return ParseJsonValueOrError(record);
        }
    }

    private static SinkRecord CreateRecord(string value, string? key = null, string topic = "input")
    {
        return new SinkRecord
        {
            Topic = topic,
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = Encoding.UTF8.GetBytes(value)
        };
    }

    [Fact]
    public void EmitError_WithErrorTopic_EmitsRecord()
    {
        var task = new TestNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["error.topic"] = "errors",
            ["node.id"] = "test-node"
        });

        var record = CreateRecord("{\"id\":1}", key: "key1");
        task.TestEmitError(record, new InvalidOperationException("test error"));

        Assert.Single(task.EmittedRecords);
        var emitted = task.EmittedRecords[0];
        Assert.Equal("errors", emitted.Topic);

        var value = JsonDocument.Parse(emitted.Value);
        Assert.Equal("test error", value.RootElement.GetProperty("error").GetString());
        Assert.Equal("test-node", value.RootElement.GetProperty("nodeId").GetString());
        Assert.Equal("InvalidOperationException", value.RootElement.GetProperty("errorType").GetString());
    }

    [Fact]
    public void EmitError_WithoutErrorTopic_NoOp()
    {
        var task = new TestNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"id\":1}");
        task.TestEmitError(record, new Exception("should not emit"));

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task ParseJsonValueOrError_ValidJson_ReturnsDocument()
    {
        var task = new TestNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["error.topic"] = "errors",
            ["node.id"] = "n1"
        });

        var record = CreateRecord("{\"name\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        // Valid JSON should emit to output, not error
        Assert.Single(task.EmittedRecords);
        Assert.Equal("output", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task ParseJsonValueOrError_InvalidJson_EmitsError()
    {
        var task = new TestNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["error.topic"] = "errors",
            ["node.id"] = "n1"
        });

        var record = CreateRecord("not valid json {{{");
        await task.PutAsync([record], CancellationToken.None);

        // Invalid JSON should emit to error topic
        Assert.Single(task.EmittedRecords);
        Assert.Equal("errors", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task ParseJsonValueOrError_InvalidJson_NoErrorTopic_BackwardCompat()
    {
        var task = new TestNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
            // No error.topic configured
        });

        var record = CreateRecord("not valid json");
        await task.PutAsync([record], CancellationToken.None);

        // Without error topic, invalid JSON should be silently dropped (backward compatible)
        Assert.Empty(task.EmittedRecords);
    }
}

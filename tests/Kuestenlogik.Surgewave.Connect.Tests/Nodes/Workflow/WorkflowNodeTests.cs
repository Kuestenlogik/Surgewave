namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Workflow;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Workflow;

public class WorkflowNodeTests
{
    private static SinkRecord CreateRecord(string value, string? key = null, Dictionary<string, byte[]>? headers = null, string topic = "input-topic")
    {
        return new SinkRecord
        {
            Topic = topic,
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = Encoding.UTF8.GetBytes(value),
            Headers = headers
        };
    }

    #region LoopNode Tests

    private static LoopNodeTask CreateLoopTask(
        int maxIterations = 5,
        string conditionField = "status",
        string conditionValue = "done",
        string conditionOperator = "equals",
        string loopTopic = "loop-back",
        string outputTopic = "output")
    {
        var task = new LoopNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["max.iterations"] = maxIterations.ToString(),
            ["condition.field"] = conditionField,
            ["condition.value"] = conditionValue,
            ["condition.operator"] = conditionOperator,
            ["loop.topic"] = loopTopic,
            ["output.topic"] = outputTopic,
            ["iteration.header"] = "x-loop-iteration"
        });
        return task;
    }

    [Fact]
    public async Task LoopNode_ConditionMet_EmitsToOutput()
    {
        var task = CreateLoopTask();

        var record = CreateRecord("""{"status":"done","data":"hello"}""", key: "k1");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("output", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task LoopNode_ConditionNotMet_EmitsToLoopTopic()
    {
        var task = CreateLoopTask();

        var record = CreateRecord("""{"status":"pending","data":"hello"}""", key: "k1");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("loop-back", task.EmittedRecords[0].Topic);

        // Check iteration header was set
        var headers = task.EmittedRecords[0].Headers;
        Assert.NotNull(headers);
        Assert.True(headers!.ContainsKey("x-loop-iteration"));
        Assert.Equal("1", Encoding.UTF8.GetString(headers["x-loop-iteration"]));
    }

    [Fact]
    public async Task LoopNode_MaxIterations_ForcesExit()
    {
        var task = CreateLoopTask(maxIterations: 3);

        // Simulate a record that has already been through 3 iterations
        var headers = new Dictionary<string, byte[]>
        {
            ["x-loop-iteration"] = Encoding.UTF8.GetBytes("3")
        };
        var record = CreateRecord("""{"status":"pending","data":"exhausted"}""", key: "k1", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("output", task.EmittedRecords[0].Topic);

        // Check exhausted header
        var emittedHeaders = task.EmittedRecords[0].Headers;
        Assert.NotNull(emittedHeaders);
        Assert.True(emittedHeaders!.ContainsKey("x-loop-exhausted"));
        Assert.Equal("true", Encoding.UTF8.GetString(emittedHeaders["x-loop-exhausted"]));
    }

    [Fact]
    public async Task LoopNode_TracksIterationHeader()
    {
        var task = CreateLoopTask();

        // Record with existing iteration count of 2
        var headers = new Dictionary<string, byte[]>
        {
            ["x-loop-iteration"] = Encoding.UTF8.GetBytes("2")
        };
        var record = CreateRecord("""{"status":"pending"}""", key: "k1", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("loop-back", task.EmittedRecords[0].Topic);

        var emittedHeaders = task.EmittedRecords[0].Headers;
        Assert.NotNull(emittedHeaders);
        // Should be incremented to 3
        Assert.Equal("3", Encoding.UTF8.GetString(emittedHeaders!["x-loop-iteration"]));
    }

    #endregion

    #region WaitForInputNode Tests

    [Fact]
    public void WaitForInputNode_HasCorrectMetadata()
    {
        var attr = typeof(WaitForInputNode)
            .GetCustomAttributes(typeof(ConnectorMetadataAttribute), false)
            .Cast<ConnectorMetadataAttribute>()
            .Single();

        Assert.Equal("Wait for Input", attr.Name);
        Assert.Contains("external input", attr.Description);
        Assert.Contains("workflow", attr.Tags);
        Assert.Equal("PauseCircle", attr.Icon);
    }

    [Fact]
    public void WaitForInputNode_ConfigDef_HasRequiredFields()
    {
        var node = new WaitForInputNode();
        var config = node.Config;
        var keyNames = config.Keys.Select(k => k.Name).ToList();

        Assert.Contains("signal.topic", keyNames);
        Assert.Contains("correlation.field", keyNames);
        Assert.Contains("timeout.ms", keyNames);
        Assert.Contains("timeout.action", keyNames);
        Assert.Contains("output.topic", keyNames);
        Assert.Contains("merge.strategy", keyNames);
    }

    #endregion

    #region GateNode Tests

    private static GateNodeTask CreateGateTask(
        string defaultState = "open",
        bool bufferWhenClosed = true,
        int maxBufferSize = 1000,
        string signalTopic = "gate-signals",
        string outputTopic = "output")
    {
        var task = new GateNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["signal.topic"] = signalTopic,
            ["default.state"] = defaultState,
            ["buffer.when.closed"] = bufferWhenClosed.ToString(),
            ["max.buffer.size"] = maxBufferSize.ToString(),
            ["output.topic"] = outputTopic
        });
        return task;
    }

    [Fact]
    public async Task GateNode_Open_PassesThrough()
    {
        var task = CreateGateTask(defaultState: "open");

        var record = CreateRecord("""{"data":"pass"}""", key: "k1");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("output", task.EmittedRecords[0].Topic);
        Assert.True(task.IsOpen);
    }

    [Fact]
    public async Task GateNode_Closed_BuffersRecords()
    {
        var task = CreateGateTask(defaultState: "closed", bufferWhenClosed: true);

        var record = CreateRecord("""{"data":"buffered"}""", key: "k1");
        await task.PutAsync([record], CancellationToken.None);

        // No records emitted (buffered)
        Assert.Empty(task.EmittedRecords);
        Assert.False(task.IsOpen);
        Assert.Equal(1, task.BufferedCount);

        // Now open the gate
        task.SetGateState(true);

        // Buffered record should be flushed
        Assert.Single(task.EmittedRecords);
        Assert.Equal("output", task.EmittedRecords[0].Topic);
    }

    #endregion

    #region AccumulatorNode Tests

    private static AccumulatorNodeTask CreateAccumulatorTask(
        int batchSize = 10,
        long windowMs = 60000,
        string outputFormat = "array",
        string groupByField = "",
        string outputTopic = "output")
    {
        var task = new AccumulatorNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["batch.size"] = batchSize.ToString(),
            ["window.ms"] = windowMs.ToString(),
            ["output.format"] = outputFormat,
            ["group.by.field"] = groupByField,
            ["output.topic"] = outputTopic
        });
        return task;
    }

    [Fact]
    public async Task AccumulatorNode_EmitsAfterBatchSize()
    {
        var task = CreateAccumulatorTask(batchSize: 3);

        var records = Enumerable.Range(0, 3)
            .Select(i => CreateRecord($"{{\"i\":{i}}}"))
            .ToList();

        await task.PutAsync(records, CancellationToken.None);

        // Should have emitted a batch
        Assert.Single(task.EmittedRecords);
        Assert.Equal("output", task.EmittedRecords[0].Topic);

        // Verify it's a JSON array
        var value = Encoding.UTF8.GetString(task.EmittedRecords[0].Value);
        using var doc = JsonDocument.Parse(value);
        Assert.Equal(JsonValueKind.Array, doc.RootElement.ValueKind);
        Assert.Equal(3, doc.RootElement.GetArrayLength());

        // Check batch size header
        var headers = task.EmittedRecords[0].Headers;
        Assert.NotNull(headers);
        Assert.Equal("3", Encoding.UTF8.GetString(headers!["x-batch-size"]));
    }

    [Fact]
    public async Task AccumulatorNode_EmitsAfterWindow()
    {
        var task = CreateAccumulatorTask(batchSize: 100, windowMs: 50);

        // Add only 2 records (less than batch size)
        var records = Enumerable.Range(0, 2)
            .Select(i => CreateRecord($"{{\"i\":{i}}}"))
            .ToList();

        await task.PutAsync(records, CancellationToken.None);

        // Not emitted yet (under batch size)
        Assert.Empty(task.EmittedRecords);
        Assert.Equal(2, task.TotalAccumulatedCount);

        // Wait for window to elapse and trigger manually
        await Task.Delay(100);
        task.OnWindowElapsed(null);

        // Should now be emitted
        Assert.Single(task.EmittedRecords);
        var value = Encoding.UTF8.GetString(task.EmittedRecords[0].Value);
        using var doc = JsonDocument.Parse(value);
        Assert.Equal(2, doc.RootElement.GetArrayLength());
    }

    #endregion

    #region MultiOutputNode Tests

    private static MultiOutputNodeTask CreateMultiOutputTask(
        string routeField = "type",
        string defaultTopic = "default-output",
        bool broadcast = false,
        params (string value, string topic)[] routes)
    {
        var task = new MultiOutputNodeTask();
        var config = new Dictionary<string, string>
        {
            ["route.field"] = routeField,
            ["default.topic"] = defaultTopic,
            ["broadcast"] = broadcast.ToString(),
            ["output.topic"] = "output"
        };

        for (var i = 0; i < routes.Length; i++)
        {
            config[$"route.{i + 1}.value"] = routes[i].value;
            config[$"route.{i + 1}.topic"] = routes[i].topic;
        }

        task.Start(config);
        return task;
    }

    [Fact]
    public async Task MultiOutputNode_RoutesBasedOnField()
    {
        var task = CreateMultiOutputTask(
            routeField: "type",
            defaultTopic: "default-output",
            broadcast: false,
            ("order", "orders-topic"),
            ("payment", "payments-topic"));

        var orderRecord = CreateRecord("""{"type":"order","amount":100}""", key: "k1");
        var paymentRecord = CreateRecord("""{"type":"payment","amount":50}""", key: "k2");
        var unknownRecord = CreateRecord("""{"type":"unknown","data":"x"}""", key: "k3");

        await task.PutAsync([orderRecord, paymentRecord, unknownRecord], CancellationToken.None);

        Assert.Equal(3, task.EmittedRecords.Count);
        Assert.Equal("orders-topic", task.EmittedRecords[0].Topic);
        Assert.Equal("payments-topic", task.EmittedRecords[1].Topic);
        Assert.Equal("default-output", task.EmittedRecords[2].Topic);
    }

    [Fact]
    public async Task MultiOutputNode_Broadcast_EmitsToAll()
    {
        var task = CreateMultiOutputTask(
            routeField: "type",
            defaultTopic: "default-output",
            broadcast: true,
            ("order", "orders-topic"),
            ("payment", "payments-topic"),
            ("audit", "audit-topic"));

        var record = CreateRecord("""{"type":"order","data":"test"}""", key: "k1");
        await task.PutAsync([record], CancellationToken.None);

        // Should emit to ALL 3 route topics
        Assert.Equal(3, task.EmittedRecords.Count);
        var topics = task.EmittedRecords.Select(r => r.Topic).ToList();
        Assert.Contains("orders-topic", topics);
        Assert.Contains("payments-topic", topics);
        Assert.Contains("audit-topic", topics);
    }

    #endregion

    #region StateNode Tests

    private static StateNodeTask CreateStateTask(
        string operation = "read_write",
        string keyField = "id",
        string valueField = "value",
        string outputField = "state",
        string stateTopic = "state-store",
        string outputTopic = "output",
        long ttlMs = 0)
    {
        var task = new StateNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["state.topic"] = stateTopic,
            ["operation"] = operation,
            ["key.field"] = keyField,
            ["value.field"] = valueField,
            ["output.field"] = outputField,
            ["output.topic"] = outputTopic,
            ["ttl.ms"] = ttlMs.ToString(),
            ["error.topic"] = "errors"
        });
        return task;
    }

    [Fact]
    public async Task StateNode_WriteAndRead_Roundtrip()
    {
        var task = CreateStateTask(operation: "read_write");

        // Write a value
        var writeRecord = CreateRecord("""{"id":"user-1","value":"hello"}""", key: "k1");
        await task.PutAsync([writeRecord], CancellationToken.None);

        // The write should emit to state topic + output
        Assert.True(task.EmittedRecords.Count >= 1);
        Assert.Equal(1, task.CacheCount);
        Assert.Equal("hello", task.GetCachedValue("user-1"));

        // Clear emitted records for the read test
        task.EmittedRecords.Clear();

        // Read the value back
        var readRecord = CreateRecord("""{"id":"user-1","value":"world"}""", key: "k2");
        await task.PutAsync([readRecord], CancellationToken.None);

        // Should have emitted with the state merged in
        var outputRecords = task.EmittedRecords.Where(r => r.Topic == "output").ToList();
        Assert.NotEmpty(outputRecords);

        var outputValue = Encoding.UTF8.GetString(outputRecords[0].Value);
        using var doc = JsonDocument.Parse(outputValue);
        Assert.True(doc.RootElement.TryGetProperty("state", out var stateElement));
        // The state should be the previously written "hello" (read_write reads first, then writes)
        Assert.Equal("hello", stateElement.GetString());
    }

    [Fact]
    public void StateNode_HasCorrectMetadata()
    {
        var attr = typeof(StateNode)
            .GetCustomAttributes(typeof(ConnectorMetadataAttribute), false)
            .Cast<ConnectorMetadataAttribute>()
            .Single();

        Assert.Equal("State Store", attr.Name);
        Assert.Contains("persistent", attr.Description);
        Assert.Contains("workflow", attr.Tags);
        Assert.Equal("Storage", attr.Icon);
    }

    #endregion
}

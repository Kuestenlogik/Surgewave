namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Logic;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

public class PriorityNodeTests
{
    private static SinkRecord CreateRecord(string value, string? key = null)
    {
        return new SinkRecord
        {
            Topic = "input-topic",
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = Encoding.UTF8.GetBytes(value)
        };
    }

    private static PriorityNodeTask CreateTask(
        string priorityField = "$.priority",
        int defaultPriority = 100,
        string order = "asc",
        long flushIntervalMs = 60000,
        int flushBatchSize = 0,
        bool flushOnPut = false)
    {
        var task = new PriorityNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["priority.field"] = priorityField,
            ["priority.default"] = defaultPriority.ToString(),
            ["priority.order"] = order,
            ["flush.interval.ms"] = flushIntervalMs.ToString(),
            ["flush.batch.size"] = flushBatchSize.ToString(),
            ["flush.on.put"] = flushOnPut.ToString()
        });
        return task;
    }

    private static string GetValue(PriorityNodeTask task, int index) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task AscOrder_LowerPriorityFirst()
    {
        var task = CreateTask(order: "asc", flushOnPut: true);

        var r1 = CreateRecord("""{"priority":3,"name":"low"}""");
        var r2 = CreateRecord("""{"priority":1,"name":"high"}""");
        var r3 = CreateRecord("""{"priority":2,"name":"mid"}""");

        await task.PutAsync([r1, r2, r3], CancellationToken.None);

        Assert.Equal(3, task.EmittedRecords.Count);
        Assert.Contains("\"name\":\"high\"", GetValue(task, 0));
        Assert.Contains("\"name\":\"mid\"", GetValue(task, 1));
        Assert.Contains("\"name\":\"low\"", GetValue(task, 2));
    }

    [Fact]
    public async Task DescOrder_HigherPriorityFirst()
    {
        var task = CreateTask(order: "desc", flushOnPut: true);

        var r1 = CreateRecord("""{"priority":1,"name":"low"}""");
        var r2 = CreateRecord("""{"priority":3,"name":"high"}""");
        var r3 = CreateRecord("""{"priority":2,"name":"mid"}""");

        await task.PutAsync([r1, r2, r3], CancellationToken.None);

        Assert.Equal(3, task.EmittedRecords.Count);
        Assert.Contains("\"name\":\"high\"", GetValue(task, 0));
        Assert.Contains("\"name\":\"mid\"", GetValue(task, 1));
        Assert.Contains("\"name\":\"low\"", GetValue(task, 2));
    }

    [Fact]
    public async Task FlushOnTimer_NotOnPut()
    {
        var task = CreateTask(flushOnPut: false, flushIntervalMs: 60000);

        var r1 = CreateRecord("""{"priority":1}""");
        await task.PutAsync([r1], CancellationToken.None);

        // Not emitted yet (no flush on put)
        Assert.Empty(task.EmittedRecords);
        Assert.Equal(1, task.BufferCount);

        // Manual flush
        task.OnFlushTimer(null);
        Assert.Single(task.EmittedRecords);
        Assert.Equal(0, task.BufferCount);
    }

    [Fact]
    public async Task FlushBatchSize_EmitsOnlyBatchCount()
    {
        var task = CreateTask(flushOnPut: false, flushBatchSize: 2);

        var records = Enumerable.Range(1, 5)
            .Select(i => CreateRecord($"{{\"priority\":{i}}}"))
            .ToList();

        await task.PutAsync(records, CancellationToken.None);
        Assert.Empty(task.EmittedRecords);

        task.OnFlushTimer(null);
        Assert.Equal(2, task.EmittedRecords.Count);
        Assert.Equal(3, task.BufferCount);

        task.OnFlushTimer(null);
        Assert.Equal(4, task.EmittedRecords.Count);
        Assert.Equal(1, task.BufferCount);
    }

    [Fact]
    public async Task MissingPriorityField_UsesDefault()
    {
        var task = CreateTask(defaultPriority: 50, flushOnPut: true);

        var r1 = CreateRecord("""{"priority":1,"name":"has-pri"}""");
        var r2 = CreateRecord("""{"name":"no-pri"}"""); // no priority field

        await task.PutAsync([r1, r2], CancellationToken.None);

        Assert.Equal(2, task.EmittedRecords.Count);
        // r1 (priority=1) should come first, r2 (default=50) second
        Assert.Contains("\"name\":\"has-pri\"", GetValue(task, 0));
        Assert.Contains("\"name\":\"no-pri\"", GetValue(task, 1));
    }

    [Fact]
    public async Task PriorityHeader_AddedToOutput()
    {
        var task = CreateTask(flushOnPut: true);

        var r1 = CreateRecord("""{"priority":42}""");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var headers = task.EmittedRecords[0].Headers;
        Assert.NotNull(headers);
        Assert.True(headers!.ContainsKey("_priority"));
        Assert.Equal("42", Encoding.UTF8.GetString(headers["_priority"]));
    }

    [Fact]
    public async Task NonJsonValue_UsesDefaultPriority()
    {
        var task = CreateTask(defaultPriority: 99, flushOnPut: true);

        var r1 = CreateRecord("not-json");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var headers = task.EmittedRecords[0].Headers;
        Assert.NotNull(headers);
        Assert.Equal("99", Encoding.UTF8.GetString(headers!["_priority"]));
    }

    [Fact]
    public async Task CustomPriorityField_ExtractsFromNestedPath()
    {
        var task = CreateTask(priorityField: "$.meta.urgency", flushOnPut: true);

        var r1 = CreateRecord("""{"meta":{"urgency":5},"data":"a"}""");
        var r2 = CreateRecord("""{"meta":{"urgency":1},"data":"b"}""");

        await task.PutAsync([r1, r2], CancellationToken.None);

        Assert.Equal(2, task.EmittedRecords.Count);
        Assert.Contains("\"data\":\"b\"", GetValue(task, 0)); // urgency 1 first
        Assert.Contains("\"data\":\"a\"", GetValue(task, 1)); // urgency 5 second
    }

    [Fact]
    public async Task Stop_FlushesRemainingBuffer()
    {
        var task = CreateTask(flushOnPut: false);

        var r1 = CreateRecord("""{"priority":1}""");
        await task.PutAsync([r1], CancellationToken.None);
        Assert.Empty(task.EmittedRecords);

        task.Stop();
        Assert.Single(task.EmittedRecords);
    }

    [Fact]
    public async Task EmptyBuffer_FlushDoesNothing()
    {
        var task = CreateTask(flushOnPut: true);
        task.OnFlushTimer(null);
        Assert.Empty(task.EmittedRecords);
    }
}

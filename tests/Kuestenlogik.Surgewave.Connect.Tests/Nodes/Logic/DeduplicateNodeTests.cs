namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Logic;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

public class DeduplicateNodeTests
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

    private static DeduplicateNodeTask CreateTask(
        string keyPath = "",
        string strategy = "first",
        long windowMs = 300000)
    {
        var task = new DeduplicateNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["dedup.key"] = keyPath,
            ["dedup.strategy"] = strategy,
            ["dedup.window.ms"] = windowMs.ToString()
        });
        return task;
    }

    [Fact]
    public async Task FirstStrategy_DuplicateKey_EmitsOnlyFirst()
    {
        var task = CreateTask(strategy: "first");

        var r1 = CreateRecord("""{"a":1}""", key: "k1");
        var r2 = CreateRecord("""{"a":2}""", key: "k1");

        await task.PutAsync([r1, r2], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"a\":1", Encoding.UTF8.GetString(task.EmittedRecords[0].Value));
    }

    [Fact]
    public async Task FirstStrategy_UniqueKeys_EmitsAll()
    {
        var task = CreateTask(strategy: "first");

        var r1 = CreateRecord("""{"a":1}""", key: "k1");
        var r2 = CreateRecord("""{"a":2}""", key: "k2");

        await task.PutAsync([r1, r2], CancellationToken.None);

        Assert.Equal(2, task.EmittedRecords.Count);
    }

    [Fact]
    public async Task LastStrategy_EmitsOnWindowExpiry()
    {
        // Use a large window so the timer doesn't fire before our manual call
        var task = CreateTask(strategy: "last", windowMs: 100);

        var r1 = CreateRecord("""{"a":1}""", key: "k1");
        var r2 = CreateRecord("""{"a":2}""", key: "k1");

        await task.PutAsync([r1], CancellationToken.None);
        await task.PutAsync([r2], CancellationToken.None);

        // Wait for window to expire
        await Task.Delay(300);

        // Trigger expiry check
        task.OnWindowExpired(null);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"a\":2", Encoding.UTF8.GetString(task.EmittedRecords[0].Value));
    }

    [Fact]
    public async Task JsonPathKey_ExtractsKeyFromValue()
    {
        var task = CreateTask(keyPath: "$.userId", strategy: "first");

        var r1 = CreateRecord("""{"userId":"u1","data":"a"}""");
        var r2 = CreateRecord("""{"userId":"u1","data":"b"}""");
        var r3 = CreateRecord("""{"userId":"u2","data":"c"}""");

        await task.PutAsync([r1, r2, r3], CancellationToken.None);

        Assert.Equal(2, task.EmittedRecords.Count);
    }

    [Fact]
    public async Task RecordKey_UsedWhenNoJsonPath()
    {
        var task = CreateTask(strategy: "first");

        var r1 = CreateRecord("""{"x":1}""", key: "same");
        var r2 = CreateRecord("""{"x":2}""", key: "same");

        await task.PutAsync([r1, r2], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
    }

    [Fact]
    public async Task WindowExpiry_CleansUpExpiredEntries()
    {
        var task = CreateTask(strategy: "first", windowMs: 1);

        var r1 = CreateRecord("""{"a":1}""", key: "k1");
        await task.PutAsync([r1], CancellationToken.None);
        Assert.Single(task.EmittedRecords);

        // Wait for expiry
        await Task.Delay(50);
        task.OnWindowExpired(null);

        // Now same key should be accepted again
        var r2 = CreateRecord("""{"a":2}""", key: "k1");
        await task.PutAsync([r2], CancellationToken.None);
        Assert.Equal(2, task.EmittedRecords.Count);
    }

    [Fact]
    public async Task NoOutputTopic_NoEmission()
    {
        var task = new DeduplicateNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["dedup.key"] = "",
            ["dedup.strategy"] = "first",
            ["dedup.window.ms"] = "300000"
        });

        var r1 = CreateRecord("""{"a":1}""", key: "k1");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task NonJsonValues_NullKeySkipped()
    {
        var task = CreateTask(keyPath: "$.id", strategy: "first");

        var r1 = CreateRecord("not-json");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }
}

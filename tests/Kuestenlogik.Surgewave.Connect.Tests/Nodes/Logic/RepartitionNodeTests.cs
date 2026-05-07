namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Logic;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

public class RepartitionNodeTests
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
            Value = Encoding.UTF8.GetBytes(value),
            Headers = new Dictionary<string, byte[]>
            {
                ["x-trace"] = Encoding.UTF8.GetBytes("abc")
            }
        };
    }

    private static RepartitionNodeTask CreateTask(
        string strategy = "key-hash",
        string partitionField = "",
        int staticPartition = 0,
        int partitionCount = 0,
        string keyField = "",
        bool preserveHeaders = true,
        string outputTopic = "output")
    {
        var task = new RepartitionNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = outputTopic,
            ["partition.strategy"] = strategy,
            ["partition.field"] = partitionField,
            ["partition.static"] = staticPartition.ToString(),
            ["partition.count"] = partitionCount.ToString(),
            ["key.field"] = keyField,
            ["preserve.headers"] = preserveHeaders.ToString()
        });
        return task;
    }

    [Fact]
    public async Task StaticPartition_AllRecordsGoToSamePartition()
    {
        var task = CreateTask(strategy: "static", staticPartition: 3);

        var r1 = CreateRecord("""{"a":1}""", key: "k1");
        var r2 = CreateRecord("""{"a":2}""", key: "k2");

        await task.PutAsync([r1, r2], CancellationToken.None);

        Assert.Equal(2, task.EmittedRecords.Count);
        Assert.All(task.EmittedRecords, r =>
        {
            Assert.Equal(3, r.Partition);
            Assert.Equal("output", r.Topic);
        });
    }

    [Fact]
    public async Task RoundRobin_DistributesAcrossPartitions()
    {
        var task = CreateTask(strategy: "round-robin", partitionCount: 3);

        var records = Enumerable.Range(0, 6)
            .Select(i => CreateRecord($"{{\"i\":{i}}}", key: $"k{i}"))
            .ToList();

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(6, task.EmittedRecords.Count);
        var partitions = task.EmittedRecords.Select(r => r.Partition).ToList();
        // Should cycle through 0, 1, 2 (modulo based on Interlocked.Increment starting at 0)
        Assert.Contains(0, partitions);
        Assert.Contains(1, partitions);
        Assert.Contains(2, partitions);
    }

    [Fact]
    public async Task FieldHash_PartitionsByJsonField()
    {
        var task = CreateTask(strategy: "field-hash", partitionField: "$.region", partitionCount: 4);

        var r1 = CreateRecord("""{"region":"EU","data":"a"}""", key: "k1");
        var r2 = CreateRecord("""{"region":"EU","data":"b"}""", key: "k2");
        var r3 = CreateRecord("""{"region":"US","data":"c"}""", key: "k3");

        await task.PutAsync([r1, r2, r3], CancellationToken.None);

        Assert.Equal(3, task.EmittedRecords.Count);
        // Same region should get same partition
        Assert.Equal(task.EmittedRecords[0].Partition, task.EmittedRecords[1].Partition);
    }

    [Fact]
    public async Task KeyHash_PartitionsByRecordKey()
    {
        var task = CreateTask(strategy: "key-hash", partitionCount: 10);

        var r1 = CreateRecord("""{"data":"a"}""", key: "same-key");
        var r2 = CreateRecord("""{"data":"b"}""", key: "same-key");

        await task.PutAsync([r1, r2], CancellationToken.None);

        Assert.Equal(2, task.EmittedRecords.Count);
        Assert.Equal(task.EmittedRecords[0].Partition, task.EmittedRecords[1].Partition);
    }

    [Fact]
    public async Task KeyField_OverridesRecordKey()
    {
        var task = CreateTask(strategy: "key-hash", keyField: "$.userId", partitionCount: 0);

        var r1 = CreateRecord("""{"userId":"u42","data":"x"}""", key: "original-key");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var emittedKey = Encoding.UTF8.GetString(task.EmittedRecords[0].Key!);
        Assert.Equal("u42", emittedKey);
    }

    [Fact]
    public async Task PreserveHeaders_True_KeepsOriginalHeaders()
    {
        var task = CreateTask(preserveHeaders: true);

        var r1 = CreateRecord("""{"a":1}""", key: "k1");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.NotNull(task.EmittedRecords[0].Headers);
        Assert.True(task.EmittedRecords[0].Headers!.ContainsKey("x-trace"));
    }

    [Fact]
    public async Task PreserveHeaders_False_DropsHeaders()
    {
        var task = CreateTask(preserveHeaders: false);

        var r1 = CreateRecord("""{"a":1}""", key: "k1");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Null(task.EmittedRecords[0].Headers);
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = CreateTask(outputTopic: "");

        var r1 = CreateRecord("""{"a":1}""", key: "k1");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task NoPartitionCount_NullPartition()
    {
        var task = CreateTask(strategy: "key-hash", partitionCount: 0);

        var r1 = CreateRecord("""{"a":1}""", key: "k1");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Null(task.EmittedRecords[0].Partition);
    }

    [Fact]
    public async Task FieldHash_MissingField_NullPartition()
    {
        var task = CreateTask(strategy: "field-hash", partitionField: "$.missing", partitionCount: 4);

        var r1 = CreateRecord("""{"data":"hello"}""", key: "k1");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Null(task.EmittedRecords[0].Partition);
    }
}

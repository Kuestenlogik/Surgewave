namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class InsertFieldNodeTests
{
    private static SinkRecord CreateRecord(string value, string? key = null, string topic = "input", int partition = 0, long offset = 0, DateTimeOffset? timestamp = null, Dictionary<string, byte[]>? headers = null)
    {
        return new SinkRecord
        {
            Topic = topic,
            Partition = partition,
            Offset = offset,
            Timestamp = timestamp ?? DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = Encoding.UTF8.GetBytes(value),
            Headers = headers
        };
    }

    private static string GetEmittedValue(InsertFieldNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task InsertOffset_AddsOffsetField()
    {
        var task = new InsertFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["offset.field"] = "_offset"
        });

        var record = CreateRecord("{\"name\":\"test\"}", offset: 42);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"_offset\":42", value);
        Assert.Contains("\"name\":\"test\"", value);
    }

    [Fact]
    public async Task InsertPartition_AddsPartitionField()
    {
        var task = new InsertFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["partition.field"] = "_partition"
        });

        var record = CreateRecord("{\"data\":1}", partition: 7);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"_partition\":7", GetEmittedValue(task));
    }

    [Fact]
    public async Task InsertTimestamp_AddsTimestampField()
    {
        var task = new InsertFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["timestamp.field"] = "_ts"
        });

        var ts = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        var record = CreateRecord("{\"x\":1}", timestamp: ts);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"_ts\":", value);
        Assert.Contains("2025-06-15T12:00:00", value);
    }

    [Fact]
    public async Task InsertTopic_AddsTopicField()
    {
        var task = new InsertFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["topic.field"] = "_topic"
        });

        var record = CreateRecord("{\"a\":1}", topic: "my-topic");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"_topic\":\"my-topic\"", GetEmittedValue(task));
    }

    [Fact]
    public async Task InsertStaticField_AddsStaticValue()
    {
        var task = new InsertFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["static.field"] = "env",
            ["static.value"] = "production"
        });

        var record = CreateRecord("{\"id\":1}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"env\":\"production\"", GetEmittedValue(task));
    }

    [Fact]
    public async Task InsertMultipleFields_AllPresent()
    {
        var task = new InsertFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["offset.field"] = "_offset",
            ["partition.field"] = "_partition",
            ["topic.field"] = "_topic",
            ["static.field"] = "env",
            ["static.value"] = "dev"
        });

        var record = CreateRecord("{\"name\":\"test\"}", topic: "src", partition: 3, offset: 99);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"_offset\":99", value);
        Assert.Contains("\"_partition\":3", value);
        Assert.Contains("\"_topic\":\"src\"", value);
        Assert.Contains("\"env\":\"dev\"", value);
        Assert.Contains("\"name\":\"test\"", value);
    }

    [Fact]
    public async Task NonJsonValue_PassedThroughUnchanged()
    {
        var task = new InsertFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["offset.field"] = "_offset"
        });

        var record = CreateRecord("plain text not json");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("plain text not json", GetEmittedValue(task));
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new InsertFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["offset.field"] = "_offset"
        });

        var record = CreateRecord("{\"id\":1}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task NoFieldsConfigured_OriginalJsonPreserved()
    {
        var task = new InsertFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"id\":1,\"name\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"id\":1", value);
        Assert.Contains("\"name\":\"test\"", value);
    }
}

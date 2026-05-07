namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class TimestampRouterNodeTests
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

    private static string GetEmittedValue(TimestampRouterNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task DefaultFormat_AppendsDayTimestamp()
    {
        var task = new TimestampRouterNodeTask();
        task.Start(new Dictionary<string, string>());

        var ts = new DateTimeOffset(2025, 7, 15, 10, 30, 0, TimeSpan.Zero);
        var record = CreateRecord("data", topic: "events", timestamp: ts);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("events-20250715", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task CustomTimestampFormat_UsesYearMonth()
    {
        var task = new TimestampRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["timestamp.format"] = "yyyy-MM"
        });

        var ts = new DateTimeOffset(2025, 12, 1, 0, 0, 0, TimeSpan.Zero);
        var record = CreateRecord("data", topic: "logs", timestamp: ts);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("logs-2025-12", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task CustomTopicFormat_FullControl()
    {
        var task = new TimestampRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["topic.format"] = "archive-${timestamp}-${topic}",
            ["timestamp.format"] = "yyyy"
        });

        var ts = new DateTimeOffset(2024, 3, 10, 0, 0, 0, TimeSpan.Zero);
        var record = CreateRecord("data", topic: "orders", timestamp: ts);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("archive-2024-orders", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task ValueAndKeyPreserved()
    {
        var task = new TimestampRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["timestamp.format"] = "yyyyMMdd"
        });

        var ts = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var record = CreateRecord("{\"id\":42}", key: "my-key", topic: "data", timestamp: ts);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("data-20250101", task.EmittedRecords[0].Topic);
        Assert.Equal("my-key", Encoding.UTF8.GetString(task.EmittedRecords[0].Key!));
        Assert.Equal("{\"id\":42}", GetEmittedValue(task));
    }

    [Fact]
    public async Task MultipleRecords_EachRoutedByTimestamp()
    {
        var task = new TimestampRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["timestamp.format"] = "yyyyMMdd"
        });

        var records = new[]
        {
            CreateRecord("a", topic: "events", timestamp: new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero)),
            CreateRecord("b", topic: "events", timestamp: new DateTimeOffset(2025, 1, 2, 0, 0, 0, TimeSpan.Zero)),
            CreateRecord("c", topic: "events", timestamp: new DateTimeOffset(2025, 1, 3, 0, 0, 0, TimeSpan.Zero))
        };
        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(3, task.EmittedRecords.Count);
        Assert.Equal("events-20250101", task.EmittedRecords[0].Topic);
        Assert.Equal("events-20250102", task.EmittedRecords[1].Topic);
        Assert.Equal("events-20250103", task.EmittedRecords[2].Topic);
    }

    [Fact]
    public async Task TopicFormatWithoutPlaceholders_UsesLiteral()
    {
        var task = new TimestampRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["topic.format"] = "fixed-output"
        });

        var record = CreateRecord("data", topic: "input");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("fixed-output", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task HeadersPreserved()
    {
        var task = new TimestampRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["timestamp.format"] = "yyyyMMdd"
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["source"] = Encoding.UTF8.GetBytes("system-a")
        };
        var ts = new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero);
        var record = CreateRecord("val", topic: "metrics", timestamp: ts, headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("metrics-20250615", task.EmittedRecords[0].Topic);
        Assert.NotNull(task.EmittedRecords[0].Headers);
    }
}

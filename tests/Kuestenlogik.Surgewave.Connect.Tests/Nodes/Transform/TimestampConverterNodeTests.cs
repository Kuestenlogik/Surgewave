namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class TimestampConverterNodeTests
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

    private static string GetEmittedValue(TimestampConverterNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task UnixSecondsToString_ConvertsCorrectly()
    {
        var task = new TimestampConverterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["field"] = "ts",
            ["target.type"] = "string",
            ["format"] = "yyyy-MM-ddTHH:mm:ssZ"
        });

        // 1700000000 = 2023-11-14T22:13:20Z
        var record = CreateRecord("{\"ts\":1700000000,\"data\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"ts\":\"2023-11-14T22:13:20Z\"", value);
        Assert.Contains("\"data\":\"test\"", value);
    }

    [Fact]
    public async Task UnixMillisecondsToString_AutoDetected()
    {
        var task = new TimestampConverterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["field"] = "ts",
            ["target.type"] = "string",
            ["format"] = "yyyy-MM-ddTHH:mm:ssZ"
        });

        // 1700000000000 ms = 2023-11-14T22:13:20Z
        var record = CreateRecord("{\"ts\":1700000000000,\"data\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"ts\":\"2023-11-14T22:13:20Z\"", value);
    }

    [Fact]
    public async Task StringToUnix_ParsesAndConverts()
    {
        var task = new TimestampConverterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["field"] = "ts",
            ["target.type"] = "unix",
            ["format"] = "yyyy-MM-ddTHH:mm:ssZ"
        });

        var record = CreateRecord("{\"ts\":\"2023-11-14T22:13:20Z\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = JsonDocument.Parse(GetEmittedValue(task));
        Assert.Equal(1700000000L, doc.RootElement.GetProperty("ts").GetInt64());
    }

    [Fact]
    public async Task StringToUnixMs_ParsesAndConverts()
    {
        var task = new TimestampConverterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["field"] = "ts",
            ["target.type"] = "unix_ms",
            ["format"] = "yyyy-MM-ddTHH:mm:ssZ"
        });

        var record = CreateRecord("{\"ts\":\"2023-11-14T22:13:20Z\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = JsonDocument.Parse(GetEmittedValue(task));
        Assert.Equal(1700000000000L, doc.RootElement.GetProperty("ts").GetInt64());
    }

    [Fact]
    public async Task NoField_UsesRecordTimestamp()
    {
        var task = new TimestampConverterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["target.type"] = "string",
            ["format"] = "yyyy-MM-ddTHH:mm:ssZ"
        });

        var ts = new DateTimeOffset(2023, 11, 14, 22, 13, 20, TimeSpan.Zero);
        var record = CreateRecord("{\"data\":\"test\"}", timestamp: ts);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"timestamp\":\"2023-11-14T22:13:20Z\"", value);
        Assert.Contains("\"data\":\"test\"", value);
    }

    [Fact]
    public async Task NoField_RecordTimestampToUnix()
    {
        var task = new TimestampConverterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["target.type"] = "unix"
        });

        var ts = new DateTimeOffset(2023, 11, 14, 22, 13, 20, TimeSpan.Zero);
        var record = CreateRecord("{\"data\":\"test\"}", timestamp: ts);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"timestamp\":1700000000", value);
    }

    [Fact]
    public async Task FieldNotPresent_PassedThroughUnchanged()
    {
        var task = new TimestampConverterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["field"] = "missing",
            ["target.type"] = "string"
        });

        var record = CreateRecord("{\"id\":1,\"name\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("{\"id\":1,\"name\":\"test\"}", GetEmittedValue(task));
    }

    [Fact]
    public async Task NonJsonValue_PassedThroughUnchanged()
    {
        var task = new TimestampConverterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["field"] = "ts",
            ["target.type"] = "string"
        });

        var record = CreateRecord("plain text not json");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("plain text not json", GetEmittedValue(task));
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new TimestampConverterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["field"] = "ts",
            ["target.type"] = "string"
        });

        var record = CreateRecord("{\"ts\":1700000000}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task UnixToUnixMs_ConvertsCorrectly()
    {
        var task = new TimestampConverterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["field"] = "ts",
            ["target.type"] = "unix_ms"
        });

        var record = CreateRecord("{\"ts\":1700000000}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = JsonDocument.Parse(GetEmittedValue(task));
        Assert.Equal(1700000000000L, doc.RootElement.GetProperty("ts").GetInt64());
    }
}

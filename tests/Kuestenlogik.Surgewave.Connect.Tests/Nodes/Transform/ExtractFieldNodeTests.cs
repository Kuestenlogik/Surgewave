namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class ExtractFieldNodeTests
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

    private static string GetEmittedValue(ExtractFieldNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    private static string? GetEmittedKey(ExtractFieldNodeTask task, int index = 0) =>
        task.EmittedRecords[index].Key != null ? Encoding.UTF8.GetString(task.EmittedRecords[index].Key!) : null;

    [Fact]
    public async Task ExtractStringField_ReturnsFieldValue()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["extract.field"] = "$.name"
        });

        var record = CreateRecord("{\"name\":\"Alice\",\"age\":30}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("Alice", GetEmittedValue(task));
    }

    [Fact]
    public async Task ExtractNumberField_ReturnsRawValue()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["extract.field"] = "$.age"
        });

        var record = CreateRecord("{\"name\":\"Bob\",\"age\":42}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("42", GetEmittedValue(task));
    }

    [Fact]
    public async Task ExtractNestedField_ReturnsNestedValue()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["extract.field"] = "$.user.name"
        });

        var record = CreateRecord("{\"user\":{\"name\":\"Carol\",\"id\":1},\"status\":\"active\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("Carol", GetEmittedValue(task));
    }

    [Fact]
    public async Task ExtractObjectField_ReturnsJsonObject()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["extract.field"] = "$.address"
        });

        var record = CreateRecord("{\"name\":\"Dave\",\"address\":{\"city\":\"Berlin\",\"zip\":\"10115\"}}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"city\":\"Berlin\"", value);
        Assert.Contains("\"zip\":\"10115\"", value);
    }

    [Fact]
    public async Task FieldNotFound_DropsRecord()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["extract.field"] = "$.nonExistent"
        });

        var record = CreateRecord("{\"name\":\"Eve\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task NonJsonValue_DropsRecord()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["extract.field"] = "$.name"
        });

        var record = CreateRecord("plain text not json");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task NoExtractFieldConfigured_DropsAll()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"name\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["extract.field"] = "$.name"
        });

        var record = CreateRecord("{\"name\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task KeyIsPreserved()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["extract.field"] = "$.value"
        });

        var record = CreateRecord("{\"value\":\"data\"}", key: "my-key");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("my-key", GetEmittedKey(task));
        Assert.Equal("data", GetEmittedValue(task));
    }

    [Fact]
    public async Task ExtractBooleanField_ReturnsRawValue()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["extract.field"] = "$.active"
        });

        var record = CreateRecord("{\"active\":true,\"name\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("true", GetEmittedValue(task));
    }

    [Fact]
    public async Task MultipleRecords_ProcessedCorrectly()
    {
        var task = new ExtractFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["extract.field"] = "$.name"
        });

        var records = new[]
        {
            CreateRecord("{\"name\":\"Alice\"}"),
            CreateRecord("{\"id\":1}"),          // no "name" field -> dropped
            CreateRecord("{\"name\":\"Carol\"}")
        };
        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(2, task.EmittedRecords.Count);
        Assert.Equal("Alice", GetEmittedValue(task, 0));
        Assert.Equal("Carol", GetEmittedValue(task, 1));
    }
}

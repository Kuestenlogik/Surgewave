namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class ValueToKeyNodeTests
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

    private static string GetEmittedValue(ValueToKeyNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    private static string? GetEmittedKey(ValueToKeyNodeTask task, int index = 0) =>
        task.EmittedRecords[index].Key != null ? Encoding.UTF8.GetString(task.EmittedRecords[index].Key!) : null;

    [Fact]
    public async Task SingleField_ExtractsStringAsKey()
    {
        var task = new ValueToKeyNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields"] = "userId"
        });

        var record = CreateRecord("{\"userId\":\"u123\",\"name\":\"Alice\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("u123", GetEmittedKey(task));
        // Value remains unchanged
        Assert.Equal("{\"userId\":\"u123\",\"name\":\"Alice\"}", GetEmittedValue(task));
    }

    [Fact]
    public async Task SingleField_ExtractsNumberAsKey()
    {
        var task = new ValueToKeyNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields"] = "id"
        });

        var record = CreateRecord("{\"id\":42,\"name\":\"Bob\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("42", GetEmittedKey(task));
    }

    [Fact]
    public async Task MultipleFields_CreatesJsonObjectKey()
    {
        var task = new ValueToKeyNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields"] = "region,userId"
        });

        var record = CreateRecord("{\"region\":\"eu\",\"userId\":\"u1\",\"data\":\"x\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var key = GetEmittedKey(task);
        Assert.NotNull(key);
        Assert.Contains("\"region\":\"eu\"", key);
        Assert.Contains("\"userId\":\"u1\"", key);
    }

    [Fact]
    public async Task MissingField_KeepsOriginalKey()
    {
        var task = new ValueToKeyNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields"] = "nonExistent"
        });

        var record = CreateRecord("{\"name\":\"test\"}", key: "original-key");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("original-key", GetEmittedKey(task));
    }

    [Fact]
    public async Task NonJsonValue_KeepsOriginalKey()
    {
        var task = new ValueToKeyNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields"] = "id"
        });

        var record = CreateRecord("plain text", key: "my-key");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("my-key", GetEmittedKey(task));
    }

    [Fact]
    public async Task NoFieldsConfigured_KeepsOriginalKey()
    {
        var task = new ValueToKeyNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"id\":1}", key: "orig");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("orig", GetEmittedKey(task));
    }

    [Fact]
    public async Task ValueIsPreservedUnchanged()
    {
        var task = new ValueToKeyNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields"] = "id"
        });

        var json = "{\"id\":1,\"data\":\"important\"}";
        var record = CreateRecord(json);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal(json, GetEmittedValue(task));
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new ValueToKeyNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["fields"] = "id"
        });

        var record = CreateRecord("{\"id\":1}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task MultipleRecords_AllProcessed()
    {
        var task = new ValueToKeyNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields"] = "id"
        });

        var records = new[]
        {
            CreateRecord("{\"id\":\"a\",\"val\":1}"),
            CreateRecord("{\"id\":\"b\",\"val\":2}"),
            CreateRecord("{\"id\":\"c\",\"val\":3}")
        };
        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(3, task.EmittedRecords.Count);
        Assert.Equal("a", GetEmittedKey(task, 0));
        Assert.Equal("b", GetEmittedKey(task, 1));
        Assert.Equal("c", GetEmittedKey(task, 2));
    }
}

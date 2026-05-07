namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class FlattenNodeTests
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

    private static string GetEmittedValue(FlattenNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task SimpleNested_FlattensWithDotDelimiter()
    {
        var task = new FlattenNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"user\":{\"name\":\"Alice\",\"age\":30}}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"user.name\":\"Alice\"", value);
        Assert.Contains("\"user.age\":30", value);
    }

    [Fact]
    public async Task DeeplyNested_FullyFlattened()
    {
        var task = new FlattenNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"user\":{\"name\":\"Alice\",\"address\":{\"city\":\"Berlin\",\"zip\":\"10115\"}}}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"user.name\":\"Alice\"", value);
        Assert.Contains("\"user.address.city\":\"Berlin\"", value);
        Assert.Contains("\"user.address.zip\":\"10115\"", value);
    }

    [Fact]
    public async Task CustomDelimiter_UsesUnderscore()
    {
        var task = new FlattenNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["flatten.delimiter"] = "_"
        });

        var record = CreateRecord("{\"config\":{\"db\":{\"host\":\"localhost\",\"port\":5432}}}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"config_db_host\":\"localhost\"", value);
        Assert.Contains("\"config_db_port\":5432", value);
    }

    [Fact]
    public async Task ArraysPreserved_NotFlattened()
    {
        var task = new FlattenNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"user\":{\"name\":\"Bob\",\"tags\":[\"admin\",\"vip\"]}}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"user.name\":\"Bob\"", value);
        Assert.Contains("\"user.tags\":[\"admin\",\"vip\"]", value);
    }

    [Fact]
    public async Task FlatObject_NoChangeExceptReserialize()
    {
        var task = new FlattenNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"a\":1,\"b\":\"hello\",\"c\":true}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"a\":1", value);
        Assert.Contains("\"b\":\"hello\"", value);
        Assert.Contains("\"c\":true", value);
    }

    [Fact]
    public async Task NonJsonValue_PassedThroughUnchanged()
    {
        var task = new FlattenNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("plain text not json");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("plain text not json", GetEmittedValue(task));
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new FlattenNodeTask();
        task.Start(new Dictionary<string, string>());

        var record = CreateRecord("{\"user\":{\"name\":\"test\"}}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task MixedTopLevelAndNested_CorrectlyFlattened()
    {
        var task = new FlattenNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"id\":1,\"meta\":{\"source\":\"api\",\"version\":2},\"active\":true}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"id\":1", value);
        Assert.Contains("\"meta.source\":\"api\"", value);
        Assert.Contains("\"meta.version\":2", value);
        Assert.Contains("\"active\":true", value);
    }

    [Fact]
    public async Task KeyAndHeadersPreserved()
    {
        var task = new FlattenNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["source"] = Encoding.UTF8.GetBytes("test")
        };
        var record = CreateRecord("{\"nested\":{\"val\":1}}", key: "my-key", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("my-key", Encoding.UTF8.GetString(task.EmittedRecords[0].Key!));
        Assert.Contains("\"nested.val\":1", GetEmittedValue(task));
        Assert.NotNull(task.EmittedRecords[0].Headers);
    }

    [Fact]
    public async Task EmptyObject_StaysEmpty()
    {
        var task = new FlattenNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("{}", GetEmittedValue(task));
    }
}

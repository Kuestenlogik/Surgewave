namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class HoistFieldNodeTests
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

    private static string GetEmittedValue(HoistFieldNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task HoistJsonObject_WrappedUnderField()
    {
        var task = new HoistFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["hoist.field"] = "data"
        });

        var record = CreateRecord("{\"a\":1,\"b\":2}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        using var doc = JsonDocument.Parse(value);
        Assert.True(doc.RootElement.TryGetProperty("data", out var dataEl));
        Assert.Equal(JsonValueKind.Object, dataEl.ValueKind);
        Assert.Equal(1, dataEl.GetProperty("a").GetInt32());
        Assert.Equal(2, dataEl.GetProperty("b").GetInt32());
    }

    [Fact]
    public async Task HoistNonJson_WrappedAsString()
    {
        var task = new HoistFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["hoist.field"] = "data"
        });

        var record = CreateRecord("hello world");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"data\":\"hello world\"", value);
    }

    [Fact]
    public async Task HoistJsonArray_WrappedUnderField()
    {
        var task = new HoistFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["hoist.field"] = "items"
        });

        var record = CreateRecord("[1,2,3]");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        using var doc = JsonDocument.Parse(value);
        Assert.True(doc.RootElement.TryGetProperty("items", out var items));
        Assert.Equal(JsonValueKind.Array, items.ValueKind);
        Assert.Equal(3, items.GetArrayLength());
    }

    [Fact]
    public async Task NoHoistField_PassedThroughUnchanged()
    {
        var task = new HoistFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"a\":1}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("{\"a\":1}", GetEmittedValue(task));
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new HoistFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["hoist.field"] = "data"
        });

        var record = CreateRecord("{\"a\":1}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task KeyAndHeadersPreserved()
    {
        var task = new HoistFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["hoist.field"] = "payload"
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["source"] = Encoding.UTF8.GetBytes("test")
        };
        var record = CreateRecord("{\"x\":1}", key: "my-key", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("my-key", Encoding.UTF8.GetString(task.EmittedRecords[0].Key!));
        Assert.NotNull(task.EmittedRecords[0].Headers);
        var value = GetEmittedValue(task);
        using var doc = JsonDocument.Parse(value);
        Assert.True(doc.RootElement.TryGetProperty("payload", out var payload));
        Assert.Equal(1, payload.GetProperty("x").GetInt32());
    }

    [Fact]
    public async Task HoistJsonScalar_WrappedUnderField()
    {
        var task = new HoistFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["hoist.field"] = "value"
        });

        var record = CreateRecord("42");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        using var doc = JsonDocument.Parse(value);
        Assert.True(doc.RootElement.TryGetProperty("value", out var val));
        Assert.Equal(42, val.GetInt32());
    }
}

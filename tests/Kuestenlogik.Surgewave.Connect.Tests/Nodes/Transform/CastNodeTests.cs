namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class CastNodeTests
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

    private static string GetEmittedValue(CastNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task CastStringToInt32()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["cast.spec"] = "age:int32"
        });

        var record = CreateRecord("{\"name\":\"Alice\",\"age\":\"30\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"age\":30", value);
        Assert.Contains("\"name\":\"Alice\"", value);
    }

    [Fact]
    public async Task CastStringToFloat64()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["cast.spec"] = "price:float64"
        });

        var record = CreateRecord("{\"item\":\"widget\",\"price\":\"19.99\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        // Parse to verify numeric value
        using var doc = JsonDocument.Parse(value);
        Assert.Equal(19.99, doc.RootElement.GetProperty("price").GetDouble(), 2);
    }

    [Fact]
    public async Task CastNumberToString()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["cast.spec"] = "code:string"
        });

        var record = CreateRecord("{\"name\":\"item\",\"code\":42}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"code\":\"42\"", value);
    }

    [Fact]
    public async Task CastStringToBoolean()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["cast.spec"] = "active:boolean"
        });

        var record = CreateRecord("{\"name\":\"test\",\"active\":\"true\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"active\":true", value);
    }

    [Fact]
    public async Task CastMultipleFields()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["cast.spec"] = "age:int32,score:float64,active:boolean"
        });

        var record = CreateRecord("{\"age\":\"25\",\"score\":\"9.5\",\"active\":\"true\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        using var doc = JsonDocument.Parse(value);
        Assert.Equal(25, doc.RootElement.GetProperty("age").GetInt32());
        Assert.Equal(9.5, doc.RootElement.GetProperty("score").GetDouble(), 2);
        Assert.True(doc.RootElement.GetProperty("active").GetBoolean());
    }

    [Fact]
    public async Task CastNumberToBoolean_ZeroIsFalse()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["cast.spec"] = "flag:boolean"
        });

        var record = CreateRecord("{\"flag\":0}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"flag\":false", GetEmittedValue(task));
    }

    [Fact]
    public async Task CastNumberToBoolean_NonZeroIsTrue()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["cast.spec"] = "flag:boolean"
        });

        var record = CreateRecord("{\"flag\":1}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"flag\":true", GetEmittedValue(task));
    }

    [Fact]
    public async Task NonJsonValue_PassedThroughUnchanged()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["cast.spec"] = "age:int32"
        });

        var record = CreateRecord("plain text not json");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("plain text not json", GetEmittedValue(task));
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["cast.spec"] = "age:int32"
        });

        var record = CreateRecord("{\"age\":\"30\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task NoCastSpec_PassedThroughUnchanged()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"age\":\"30\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"age\":\"30\"", GetEmittedValue(task));
    }

    [Fact]
    public async Task CastInt64_LargeNumber()
    {
        var task = new CastNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["cast.spec"] = "ts:int64"
        });

        var record = CreateRecord("{\"ts\":\"1700000000000\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        using var doc = JsonDocument.Parse(GetEmittedValue(task));
        Assert.Equal(1700000000000L, doc.RootElement.GetProperty("ts").GetInt64());
    }
}

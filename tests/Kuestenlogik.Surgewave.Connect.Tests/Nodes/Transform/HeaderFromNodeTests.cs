namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class HeaderFromNodeTests
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

    private static string GetEmittedValue(HeaderFromNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task CopyFieldToHeader_FieldRemainsInValue()
    {
        var task = new HeaderFromNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.from.fields"] = "env",
            ["header.from.headers"] = "X-Env",
            ["header.from.operation"] = "copy"
        });

        var record = CreateRecord("{\"env\":\"production\",\"id\":1}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"env\":\"production\"", value);
        Assert.Contains("\"id\":1", value);
        // Header should be set
        Assert.NotNull(task.EmittedRecords[0].Headers);
        Assert.Equal("production", Encoding.UTF8.GetString(task.EmittedRecords[0].Headers!["X-Env"]));
    }

    [Fact]
    public async Task MoveFieldToHeader_FieldRemovedFromValue()
    {
        var task = new HeaderFromNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.from.fields"] = "env",
            ["header.from.headers"] = "X-Env",
            ["header.from.operation"] = "move"
        });

        var record = CreateRecord("{\"env\":\"staging\",\"id\":2}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.DoesNotContain("\"env\"", value);
        Assert.Contains("\"id\":2", value);
        Assert.Equal("staging", Encoding.UTF8.GetString(task.EmittedRecords[0].Headers!["X-Env"]));
    }

    [Fact]
    public async Task MultipleFields_AllCopiedToHeaders()
    {
        var task = new HeaderFromNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.from.fields"] = "env,region",
            ["header.from.headers"] = "X-Env,X-Region"
        });

        var record = CreateRecord("{\"env\":\"prod\",\"region\":\"eu-west\",\"data\":42}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var headers = task.EmittedRecords[0].Headers!;
        Assert.Equal("prod", Encoding.UTF8.GetString(headers["X-Env"]));
        Assert.Equal("eu-west", Encoding.UTF8.GetString(headers["X-Region"]));
    }

    [Fact]
    public async Task NonJsonValue_PassedThroughUnchanged()
    {
        var task = new HeaderFromNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.from.fields"] = "env",
            ["header.from.headers"] = "X-Env"
        });

        var record = CreateRecord("plain text not json");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("plain text not json", GetEmittedValue(task));
    }

    [Fact]
    public async Task FieldNotPresent_NoHeaderAdded()
    {
        var task = new HeaderFromNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.from.fields"] = "missing",
            ["header.from.headers"] = "X-Missing"
        });

        var record = CreateRecord("{\"id\":1,\"name\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"id\":1", value);
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new HeaderFromNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["header.from.fields"] = "env",
            ["header.from.headers"] = "X-Env"
        });

        var record = CreateRecord("{\"env\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task DefaultOperation_IsCopy()
    {
        var task = new HeaderFromNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.from.fields"] = "env",
            ["header.from.headers"] = "X-Env"
            // No operation specified - should default to copy
        });

        var record = CreateRecord("{\"env\":\"dev\",\"id\":1}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"env\":\"dev\"", value); // field still present (copy, not move)
        Assert.Equal("dev", Encoding.UTF8.GetString(task.EmittedRecords[0].Headers!["X-Env"]));
    }

    [Fact]
    public async Task NumericFieldValue_ConvertedToStringInHeader()
    {
        var task = new HeaderFromNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.from.fields"] = "count",
            ["header.from.headers"] = "X-Count"
        });

        var record = CreateRecord("{\"count\":42,\"name\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.NotNull(task.EmittedRecords[0].Headers);
        Assert.Equal("42", Encoding.UTF8.GetString(task.EmittedRecords[0].Headers!["X-Count"]));
    }
}

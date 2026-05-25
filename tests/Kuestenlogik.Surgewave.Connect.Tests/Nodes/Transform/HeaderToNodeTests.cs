namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class HeaderToNodeTests
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

    private static string GetEmittedValue(HeaderToNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task CopyHeaderToField_HeaderRemains()
    {
        var task = new HeaderToNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.to.headers"] = "X-Env",
            ["header.to.fields"] = "env",
            ["header.to.operation"] = "copy"
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["X-Env"] = Encoding.UTF8.GetBytes("production")
        };
        var record = CreateRecord("{\"id\":1}", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"id\":1", value);
        Assert.Contains("\"env\":\"production\"", value);
        // Header should still be present (copy)
        Assert.NotNull(task.EmittedRecords[0].Headers);
        Assert.True(task.EmittedRecords[0].Headers!.ContainsKey("X-Env"));
    }

    [Fact]
    public async Task MoveHeaderToField_HeaderRemoved()
    {
        var task = new HeaderToNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.to.headers"] = "X-Env",
            ["header.to.fields"] = "env",
            ["header.to.operation"] = "move"
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["X-Env"] = Encoding.UTF8.GetBytes("staging"),
            ["X-Other"] = Encoding.UTF8.GetBytes("keep-me")
        };
        var record = CreateRecord("{\"id\":2}", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"env\":\"staging\"", value);
        // X-Env should be removed, X-Other should remain
        var emittedHeaders = task.EmittedRecords[0].Headers!;
        Assert.False(emittedHeaders.ContainsKey("X-Env"));
        Assert.True(emittedHeaders.ContainsKey("X-Other"));
    }

    [Fact]
    public async Task MultipleHeaders_AllInjectedIntoFields()
    {
        var task = new HeaderToNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.to.headers"] = "X-Env,X-Region",
            ["header.to.fields"] = "env,region"
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["X-Env"] = Encoding.UTF8.GetBytes("prod"),
            ["X-Region"] = Encoding.UTF8.GetBytes("eu-west")
        };
        var record = CreateRecord("{\"data\":42}", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"env\":\"prod\"", value);
        Assert.Contains("\"region\":\"eu-west\"", value);
        Assert.Contains("\"data\":42", value);
    }

    [Fact]
    public async Task NonJsonValue_PassedThroughUnchanged()
    {
        var task = new HeaderToNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.to.headers"] = "X-Env",
            ["header.to.fields"] = "env"
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["X-Env"] = Encoding.UTF8.GetBytes("test")
        };
        var record = CreateRecord("plain text not json", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("plain text not json", GetEmittedValue(task));
    }

    [Fact]
    public async Task HeaderNotPresent_FieldNotAdded()
    {
        var task = new HeaderToNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.to.headers"] = "X-Missing",
            ["header.to.fields"] = "missing"
        });

        var record = CreateRecord("{\"id\":1}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"id\":1", value);
        Assert.DoesNotContain("\"missing\"", value);
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new HeaderToNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["header.to.headers"] = "X-Env",
            ["header.to.fields"] = "env"
        });

        var record = CreateRecord("{\"id\":1}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task DefaultOperation_IsCopy()
    {
        var task = new HeaderToNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["header.to.headers"] = "X-Env",
            ["header.to.fields"] = "env"
            // No operation specified - should default to copy
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["X-Env"] = Encoding.UTF8.GetBytes("dev")
        };
        var record = CreateRecord("{\"id\":1}", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"env\":\"dev\"", GetEmittedValue(task));
        // Header should still be present (copy is default)
        Assert.True(task.EmittedRecords[0].Headers!.ContainsKey("X-Env"));
    }
}

namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class RegexRouterNodeTests
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

    private static string GetEmittedValue(RegexRouterNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task SimpleReplacement_ChangesTopicName()
    {
        var task = new RegexRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["regex.pattern"] = "^input$",
            ["regex.replacement"] = "output"
        });

        var record = CreateRecord("{\"id\":1}", topic: "input");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("output", task.EmittedRecords[0].Topic);
        Assert.Equal("{\"id\":1}", GetEmittedValue(task));
    }

    [Fact]
    public async Task CaptureGroups_SubstitutedCorrectly()
    {
        var task = new RegexRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["regex.pattern"] = "^(.+)-(.+)$",
            ["regex.replacement"] = "$2-$1-archived"
        });

        var record = CreateRecord("data", topic: "orders-eu");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("eu-orders-archived", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task PartialMatch_ReplacesMatchedPortion()
    {
        var task = new RegexRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["regex.pattern"] = "-raw$",
            ["regex.replacement"] = "-processed"
        });

        var record = CreateRecord("value", topic: "events-raw");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("events-processed", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task NoPatternConfigured_FallsBackToOutputTopic()
    {
        var task = new RegexRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "fallback"
        });

        var record = CreateRecord("data", topic: "input");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("fallback", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task NoPatternAndNoOutputTopic_EmitsNothing()
    {
        var task = new RegexRouterNodeTask();
        task.Start(new Dictionary<string, string>());

        var record = CreateRecord("data", topic: "input");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task KeyAndHeadersPreserved()
    {
        var task = new RegexRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["regex.pattern"] = "^src$",
            ["regex.replacement"] = "dst"
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["traceId"] = Encoding.UTF8.GetBytes("abc123")
        };
        var record = CreateRecord("{\"v\":1}", key: "my-key", topic: "src", headers: headers);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("dst", task.EmittedRecords[0].Topic);
        Assert.Equal("my-key", Encoding.UTF8.GetString(task.EmittedRecords[0].Key!));
        Assert.Equal("{\"v\":1}", GetEmittedValue(task));
    }

    [Fact]
    public async Task MultipleRecords_AllRouted()
    {
        var task = new RegexRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["regex.pattern"] = "^(.+)$",
            ["regex.replacement"] = "$1-copy"
        });

        var records = new[]
        {
            CreateRecord("a", topic: "topic-a"),
            CreateRecord("b", topic: "topic-b"),
            CreateRecord("c", topic: "topic-c")
        };
        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(3, task.EmittedRecords.Count);
        Assert.Equal("topic-a-copy", task.EmittedRecords[0].Topic);
        Assert.Equal("topic-b-copy", task.EmittedRecords[1].Topic);
        Assert.Equal("topic-c-copy", task.EmittedRecords[2].Topic);
    }

    [Fact]
    public async Task NoMatch_TopicUnchanged()
    {
        var task = new RegexRouterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["regex.pattern"] = "^nonexistent$",
            ["regex.replacement"] = "replaced"
        });

        var record = CreateRecord("data", topic: "my-topic");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("my-topic", task.EmittedRecords[0].Topic);
    }
}

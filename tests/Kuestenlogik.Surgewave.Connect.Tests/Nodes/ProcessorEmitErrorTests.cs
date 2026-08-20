namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes;

using System.Text;
using Kuestenlogik.Surgewave.Connect.Nodes;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

/// <summary>
/// Parse failures in processor nodes are reported to the wired error output — without an
/// error connection the record is still dropped silently (unchanged behavior).
/// </summary>
public class ProcessorEmitErrorTests
{
    private static SinkRecord Record(string value)
    {
        return new SinkRecord
        {
            Topic = "in",
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = null,
            Value = Encoding.UTF8.GetBytes(value),
            Headers = null,
        };
    }

    private static FilterNodeTask CreateTask(string? errorTopic)
    {
        var task = new FilterNodeTask();
        task.Initialize(new TaskContext());

        var config = new Dictionary<string, string>
        {
            ["output.topic"] = "out",
            ["condition"] = "$.x == 1",
            ["node.id"] = "n1",
            ["pipeline.id"] = "p1",
        };
        if (errorTopic is not null)
        {
            config["error.topic"] = errorTopic;
        }

        task.Start(config);
        return task;
    }

    [Fact]
    public async Task MalformedJson_WithErrorTopic_EmitsErrorRecord()
    {
        var task = CreateTask(errorTopic: "err");

        await task.PutAsync([Record("not json")], CancellationToken.None);

        var emitted = Assert.Single(task.EmittedRecords);
        Assert.Equal("err", emitted.Topic);
    }

    [Fact]
    public async Task MalformedJson_WithoutErrorTopic_IsDroppedSilently()
    {
        var task = CreateTask(errorTopic: null);

        await task.PutAsync([Record("not json")], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task MatchingRecord_StillEmitsToOutput()
    {
        var task = CreateTask(errorTopic: "err");

        await task.PutAsync([Record("{\"x\":1}")], CancellationToken.None);

        var emitted = Assert.Single(task.EmittedRecords);
        Assert.Equal("out", emitted.Topic);
    }
}

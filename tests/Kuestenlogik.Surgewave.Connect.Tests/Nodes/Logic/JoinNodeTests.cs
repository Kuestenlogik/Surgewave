namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Logic;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

public class JoinNodeTests
{
    private static SinkRecord CreateRecord(string value, string? key = null, string topic = "left-topic")
    {
        return new SinkRecord
        {
            Topic = topic,
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = Encoding.UTF8.GetBytes(value)
        };
    }

    private static string GetEmittedValue(JoinNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    private static JoinNodeTask CreateTask(
        string joinType = "inner",
        string leftKey = "",
        string rightKey = "",
        string leftTopic = "left-topic",
        string rightTopic = "right-topic")
    {
        var task = new JoinNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["join.type"] = joinType,
            ["join.window.ms"] = "60000",
            ["join.key.left"] = leftKey,
            ["join.key.right"] = rightKey,
            ["left.topic"] = leftTopic,
            ["right.topic"] = rightTopic
        });
        return task;
    }

    [Fact]
    public async Task InnerJoin_MatchedRecords_EmitsJoined()
    {
        var task = CreateTask(leftKey: "$.userId", rightKey: "$.id");

        var left = CreateRecord("""{"userId":"u1","order":"A"}""", topic: "left-topic");
        var right = CreateRecord("""{"id":"u1","name":"Alice"}""", topic: "right-topic");

        await task.PutAsync([left], CancellationToken.None);
        Assert.Empty(task.EmittedRecords); // No match yet

        await task.PutAsync([right], CancellationToken.None);
        Assert.Single(task.EmittedRecords);

        var value = GetEmittedValue(task);
        Assert.Contains("\"userId\":\"u1\"", value);
        Assert.Contains("\"name\":\"Alice\"", value);
        Assert.Contains("\"_join_key\":\"u1\"", value);
    }

    [Fact]
    public async Task InnerJoin_UnmatchedRecords_NoOutput()
    {
        var task = CreateTask(leftKey: "$.userId", rightKey: "$.id");

        var left = CreateRecord("""{"userId":"u1","order":"A"}""", topic: "left-topic");
        var right = CreateRecord("""{"id":"u2","name":"Bob"}""", topic: "right-topic");

        await task.PutAsync([left], CancellationToken.None);
        await task.PutAsync([right], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task LeftJoin_MatchedRecords_EmitsJoined()
    {
        var task = CreateTask(joinType: "left", leftKey: "$.userId", rightKey: "$.id");

        var left = CreateRecord("""{"userId":"u1","order":"A"}""", topic: "left-topic");
        var right = CreateRecord("""{"id":"u1","name":"Alice"}""", topic: "right-topic");

        await task.PutAsync([left], CancellationToken.None);
        await task.PutAsync([right], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"name\":\"Alice\"", value);
    }

    [Fact]
    public async Task InnerJoin_RightFirst_ThenLeft_EmitsJoined()
    {
        var task = CreateTask(leftKey: "$.id", rightKey: "$.id");

        var right = CreateRecord("""{"id":"x1","detail":"info"}""", topic: "right-topic");
        var left = CreateRecord("""{"id":"x1","main":"data"}""", topic: "left-topic");

        await task.PutAsync([right], CancellationToken.None);
        Assert.Empty(task.EmittedRecords);

        await task.PutAsync([left], CancellationToken.None);
        Assert.Single(task.EmittedRecords);
    }

    [Fact]
    public async Task Join_UsingRecordKey_WhenNoJsonPath()
    {
        var task = CreateTask(); // No key paths = use record key

        var left = CreateRecord("""{"order":"A"}""", key: "k1", topic: "left-topic");
        var right = CreateRecord("""{"name":"Alice"}""", key: "k1", topic: "right-topic");

        await task.PutAsync([left], CancellationToken.None);
        await task.PutAsync([right], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"order\":\"A\"", value);
        Assert.Contains("\"name\":\"Alice\"", value);
    }

    [Fact]
    public async Task Join_WithPrefixes_PrefixesFields()
    {
        var task = new JoinNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["join.type"] = "inner",
            ["join.window.ms"] = "60000",
            ["join.key.left"] = "",
            ["join.key.right"] = "",
            ["left.topic"] = "left-topic",
            ["right.topic"] = "right-topic",
            ["output.left.prefix"] = "order.",
            ["output.right.prefix"] = "user."
        });

        var left = CreateRecord("""{"id":1}""", key: "k1", topic: "left-topic");
        var right = CreateRecord("""{"id":2}""", key: "k1", topic: "right-topic");

        await task.PutAsync([left], CancellationToken.None);
        await task.PutAsync([right], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"order.id\":1", value);
        Assert.Contains("\"user.id\":2", value);
    }

    [Fact]
    public async Task Join_OutputContainsMetadata()
    {
        var task = CreateTask(leftKey: "$.id", rightKey: "$.id");

        var left = CreateRecord("""{"id":"k1","a":1}""", topic: "left-topic");
        var right = CreateRecord("""{"id":"k1","b":2}""", topic: "right-topic");

        await task.PutAsync([left], CancellationToken.None);
        await task.PutAsync([right], CancellationToken.None);

        var value = GetEmittedValue(task);
        Assert.Contains("\"_join_key\":\"k1\"", value);
        Assert.Contains("\"_join_timestamp\":", value);
    }

    [Fact]
    public async Task Join_MultipleRecords_MatchesCorrectly()
    {
        var task = CreateTask(leftKey: "$.id", rightKey: "$.id");

        var left1 = CreateRecord("""{"id":"a","val":1}""", topic: "left-topic");
        var left2 = CreateRecord("""{"id":"b","val":2}""", topic: "left-topic");
        var right1 = CreateRecord("""{"id":"b","info":"B"}""", topic: "right-topic");

        await task.PutAsync([left1, left2], CancellationToken.None);
        await task.PutAsync([right1], CancellationToken.None);

        // Only left2 should match with right1
        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"val\":2", value);
        Assert.Contains("\"info\":\"B\"", value);
    }

    [Fact]
    public async Task Join_NullKey_SkipsRecord()
    {
        var task = CreateTask(leftKey: "$.missing", rightKey: "$.missing");

        var left = CreateRecord("""{"id":"a"}""", topic: "left-topic");
        var right = CreateRecord("""{"id":"a"}""", topic: "right-topic");

        await task.PutAsync([left], CancellationToken.None);
        await task.PutAsync([right], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task Join_NumericKey_Matches()
    {
        var task = CreateTask(leftKey: "$.id", rightKey: "$.id");

        var left = CreateRecord("""{"id":42,"name":"item"}""", topic: "left-topic");
        var right = CreateRecord("""{"id":42,"detail":"info"}""", topic: "right-topic");

        await task.PutAsync([left], CancellationToken.None);
        await task.PutAsync([right], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
    }

    [Fact]
    public async Task Join_NoOutputTopic_EmitsNothing()
    {
        var task = new JoinNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["join.type"] = "inner",
            ["join.key.left"] = "",
            ["join.key.right"] = "",
            ["left.topic"] = "left-topic",
            ["right.topic"] = "right-topic"
        });

        var left = CreateRecord("""{"a":1}""", key: "k1", topic: "left-topic");
        var right = CreateRecord("""{"b":2}""", key: "k1", topic: "right-topic");

        await task.PutAsync([left], CancellationToken.None);
        await task.PutAsync([right], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public void Stop_ClearsState()
    {
        var task = CreateTask();
        task.Stop();
        // No exception thrown = success
    }
}

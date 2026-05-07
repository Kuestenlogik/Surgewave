using Kuestenlogik.Surgewave.Streams.Sql;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests.Sql;

public sealed class SqlTopicSourceTests
{
    private readonly ITestOutputHelper _output;

    public SqlTopicSourceTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void TopicSource_DeserializesJsonMessages()
    {
        var messages = new List<RawTopicMessage>
        {
            new(Offset: 0, Partition: 0, Timestamp: DateTimeOffset.UtcNow,
                Key: "k1", Value: """{"name":"Alice","age":30}"""),
            new(Offset: 1, Partition: 0, Timestamp: DateTimeOffset.UtcNow,
                Key: "k2", Value: """{"name":"Bob","age":25}"""),
        };

        var source = new SqlTopicSource(messages);
        var rows = source.ToList();

        Assert.Equal(2, rows.Count);

        // Check JSON fields are extracted
        Assert.Equal("Alice", rows[0]["name"]);
        Assert.Equal(30L, rows[0]["age"]);

        // Check metadata columns
        Assert.Equal(0L, rows[0]["_offset"]);
        Assert.Equal(0, rows[0]["_partition"]);
        Assert.Equal("k1", rows[0]["_key"]);
    }

    [Fact]
    public void TopicSource_RespectsLimit()
    {
        var messages = new List<RawTopicMessage>
        {
            new(0, 0, DateTimeOffset.UtcNow, null, """{"x":1}"""),
            new(1, 0, DateTimeOffset.UtcNow, null, """{"x":2}"""),
            new(2, 0, DateTimeOffset.UtcNow, null, """{"x":3}"""),
            new(3, 0, DateTimeOffset.UtcNow, null, """{"x":4}"""),
            new(4, 0, DateTimeOffset.UtcNow, null, """{"x":5}"""),
        };

        var source = new SqlTopicSource(messages, limit: 3);
        var rows = source.ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal(1L, rows[0]["x"]);
        Assert.Equal(3L, rows[2]["x"]);
    }

    [Fact]
    public void TopicSource_HandlesNonJsonValues()
    {
        var messages = new List<RawTopicMessage>
        {
            new(0, 0, DateTimeOffset.UtcNow, "key1", "plain text value"),
        };

        var source = new SqlTopicSource(messages);
        var rows = source.ToList();

        Assert.Single(rows);
        // Non-JSON value should be stored as _value
        Assert.Equal("plain text value", rows[0]["_value"]);
        Assert.Equal("key1", rows[0]["_key"]);
    }
}

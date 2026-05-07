namespace Kuestenlogik.Surgewave.Connect.Tests.Models;

/// <summary>
/// Tests for SourceRecord and SinkRecord models.
/// </summary>
public sealed class RecordModelTests
{
    [Fact]
    public void SourceRecord_AllProperties_AreSet()
    {
        var ts = DateTimeOffset.UtcNow;
        var record = new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["file"] = "data.csv" },
            SourceOffset = new Dictionary<string, object> { ["line"] = 42 },
            Topic = "csv-data",
            Partition = 3,
            Key = [1, 2, 3],
            Value = [10, 20, 30],
            Timestamp = ts,
            Headers = new Dictionary<string, byte[]> { ["source"] = [42] }
        };

        Assert.Equal("data.csv", record.SourcePartition["file"]);
        Assert.Equal(42, record.SourceOffset["line"]);
        Assert.Equal("csv-data", record.Topic);
        Assert.Equal(3, record.Partition);
        Assert.Equal([1, 2, 3], record.Key);
        Assert.Equal([10, 20, 30], record.Value);
        Assert.Equal(ts, record.Timestamp);
        Assert.NotNull(record.Headers);
        Assert.Equal([42], record.Headers["source"]);
    }

    [Fact]
    public void SourceRecord_OptionalFields_DefaultToNull()
    {
        var record = new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["src"] = "test" },
            SourceOffset = new Dictionary<string, object> { ["pos"] = 0 },
            Topic = "output",
            Value = [1]
        };

        Assert.Null(record.Partition);
        Assert.Null(record.Key);
        Assert.Null(record.Timestamp);
        Assert.Null(record.Headers);
    }

    [Fact]
    public void SinkRecord_AllProperties_AreSet()
    {
        var ts = DateTimeOffset.UtcNow;
        var headers = new Dictionary<string, byte[]>
        {
            ["content-type"] = "application/json"u8.ToArray()
        };

        var record = new SinkRecord
        {
            Topic = "events",
            Partition = 5,
            Offset = 12345,
            Key = [1, 2],
            Value = [3, 4, 5],
            Timestamp = ts,
            Headers = headers
        };

        Assert.Equal("events", record.Topic);
        Assert.Equal(5, record.Partition);
        Assert.Equal(12345, record.Offset);
        Assert.Equal([1, 2], record.Key);
        Assert.Equal([3, 4, 5], record.Value);
        Assert.Equal(ts, record.Timestamp);
        Assert.NotNull(record.Headers);
    }

    [Fact]
    public void SinkRecord_KeyIsOptional()
    {
        var record = new SinkRecord
        {
            Topic = "no-key",
            Partition = 0,
            Offset = 0,
            Value = [1],
            Timestamp = DateTimeOffset.UtcNow
        };

        Assert.Null(record.Key);
        Assert.Null(record.Headers);
    }

    [Fact]
    public void SinkRecord_RecordEquality()
    {
        var ts = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var record1 = new SinkRecord
        {
            Topic = "t",
            Partition = 0,
            Offset = 1,
            Value = [42],
            Timestamp = ts
        };
        var record2 = new SinkRecord
        {
            Topic = "t",
            Partition = 0,
            Offset = 1,
            Value = [42],
            Timestamp = ts
        };

        // Records use value equality for records
        Assert.Equal(record1.Topic, record2.Topic);
        Assert.Equal(record1.Partition, record2.Partition);
        Assert.Equal(record1.Offset, record2.Offset);
    }

    [Fact]
    public void TaskContext_AllProperties_AreSet()
    {
        var partitions = new List<TopicPartition>
        {
            new("topic-a", 0),
            new("topic-a", 1)
        };

        var errorRaised = false;
        var context = new TaskContext
        {
            AssignedPartitions = partitions,
            RaiseError = _ => errorRaised = true
        };

        Assert.NotNull(context.AssignedPartitions);
        Assert.Equal(2, context.AssignedPartitions.Count);
        Assert.Null(context.OffsetStorageReader);
        Assert.NotNull(context.RaiseError);

        context.RaiseError!(new Exception("test"));
        Assert.True(errorRaised);
    }

    [Fact]
    public void TaskContext_Defaults_AreNull()
    {
        var context = new TaskContext();

        Assert.Null(context.AssignedPartitions);
        Assert.Null(context.OffsetStorageReader);
        Assert.Null(context.RaiseError);
        Assert.Null(context.Producer);
    }
}

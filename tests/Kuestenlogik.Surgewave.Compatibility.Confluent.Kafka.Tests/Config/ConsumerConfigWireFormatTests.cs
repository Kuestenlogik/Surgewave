namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Config;

/// <summary>
/// Pins the raw librdkafka wire format of ConsumerConfig: canonical key names,
/// offset-reset/isolation-level/assignment-strategy string mappings in both
/// directions, and tolerant handling of malformed raw values.
/// </summary>
public class ConsumerConfigWireFormatTests
{
    [Theory]
    [InlineData(AutoOffsetReset.Earliest, "earliest")]
    [InlineData(AutoOffsetReset.Latest, "latest")]
    [InlineData(AutoOffsetReset.Error, "error")]
    public void AutoOffsetReset_Setter_WritesWireString(AutoOffsetReset reset, string expected)
    {
        var config = new ConsumerConfig { AutoOffsetReset = reset };
        Assert.Equal(expected, config["auto.offset.reset"]);
    }

    [Theory]
    [InlineData("earliest", AutoOffsetReset.Earliest)]
    [InlineData("latest", AutoOffsetReset.Latest)]
    [InlineData("error", AutoOffsetReset.Error)]
    public void AutoOffsetReset_Getter_ParsesWireString(string raw, AutoOffsetReset expected)
    {
        var config = new ConsumerConfig { ["auto.offset.reset"] = raw };
        Assert.Equal(expected, config.AutoOffsetReset);
    }

    [Fact]
    public void AutoOffsetReset_UnknownWireString_ReturnsNull()
    {
        var config = new ConsumerConfig { ["auto.offset.reset"] = "beginning" };
        Assert.Null(config.AutoOffsetReset);
    }

    [Theory]
    [InlineData(IsolationLevel.ReadUncommitted, "read_uncommitted")]
    [InlineData(IsolationLevel.ReadCommitted, "read_committed")]
    public void IsolationLevel_Setter_WritesWireString(IsolationLevel level, string expected)
    {
        var config = new ConsumerConfig { IsolationLevel = level };
        Assert.Equal(expected, config["isolation.level"]);
    }

    [Theory]
    [InlineData("read_uncommitted", IsolationLevel.ReadUncommitted)]
    [InlineData("read_committed", IsolationLevel.ReadCommitted)]
    public void IsolationLevel_Getter_ParsesWireString(string raw, IsolationLevel expected)
    {
        var config = new ConsumerConfig { ["isolation.level"] = raw };
        Assert.Equal(expected, config.IsolationLevel);
    }

    [Fact]
    public void IsolationLevel_UnknownWireString_ReturnsNull()
    {
        var config = new ConsumerConfig { ["isolation.level"] = "serializable" };
        Assert.Null(config.IsolationLevel);
    }

    [Theory]
    [InlineData(PartitionAssignmentStrategy.Range, "range")]
    [InlineData(PartitionAssignmentStrategy.RoundRobin, "roundrobin")]
    [InlineData(PartitionAssignmentStrategy.CooperativeSticky, "cooperative-sticky")]
    public void PartitionAssignmentStrategy_Setter_WritesWireString(
        PartitionAssignmentStrategy strategy,
        string expected)
    {
        var config = new ConsumerConfig { PartitionAssignmentStrategy = strategy };
        Assert.Equal(expected, config["partition.assignment.strategy"]);
    }

    [Theory]
    [InlineData("range", PartitionAssignmentStrategy.Range)]
    [InlineData("roundrobin", PartitionAssignmentStrategy.RoundRobin)]
    [InlineData("cooperative-sticky", PartitionAssignmentStrategy.CooperativeSticky)]
    public void PartitionAssignmentStrategy_Getter_ParsesWireString(
        string raw,
        PartitionAssignmentStrategy expected)
    {
        var config = new ConsumerConfig { ["partition.assignment.strategy"] = raw };
        Assert.Equal(expected, config.PartitionAssignmentStrategy);
    }

    [Fact]
    public void PartitionAssignmentStrategy_UnknownWireString_ReturnsNull()
    {
        var config = new ConsumerConfig { ["partition.assignment.strategy"] = "sticky" };
        Assert.Null(config.PartitionAssignmentStrategy);
    }

    [Fact]
    public void TypedProperties_WriteCanonicalLibrdkafkaKeys()
    {
        var config = new ConsumerConfig
        {
            GroupId = "group-1",
            GroupInstanceId = "instance-1",
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 5000,
            EnableAutoOffsetStore = false,
            MaxPollRecords = 500,
            MaxPollIntervalMs = 300000,
            SessionTimeoutMs = 45000,
            HeartbeatIntervalMs = 3000,
            FetchMinBytes = 1,
            MaxPartitionFetchBytes = 1048576,
            FetchWaitMaxMs = 500,
            CheckCrcs = true
        };

        Assert.Equal("group-1", config["group.id"]);
        Assert.Equal("instance-1", config["group.instance.id"]);
        Assert.Equal("true", config["enable.auto.commit"]);
        Assert.Equal("5000", config["auto.commit.interval.ms"]);
        Assert.Equal("false", config["enable.auto.offset.store"]);
        Assert.Equal("500", config["max.poll.records"]);
        Assert.Equal("300000", config["max.poll.interval.ms"]);
        Assert.Equal("45000", config["session.timeout.ms"]);
        Assert.Equal("3000", config["heartbeat.interval.ms"]);
        Assert.Equal("1", config["fetch.min.bytes"]);
        Assert.Equal("1048576", config["max.partition.fetch.bytes"]);
        Assert.Equal("500", config["fetch.wait.max.ms"]);
        Assert.Equal("true", config["check.crcs"]);
    }

    [Fact]
    public void Constructor_FromRawDictionary_ExposesTypedProperties()
    {
        var raw = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "broker:9092",
            ["group.id"] = "group-1",
            ["auto.offset.reset"] = "earliest",
            ["isolation.level"] = "read_committed",
            ["partition.assignment.strategy"] = "cooperative-sticky",
            ["enable.auto.commit"] = "false",
            ["session.timeout.ms"] = "45000"
        };

        var config = new ConsumerConfig(raw);

        Assert.Equal("broker:9092", config.BootstrapServers);
        Assert.Equal("group-1", config.GroupId);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
        Assert.Equal(IsolationLevel.ReadCommitted, config.IsolationLevel);
        Assert.Equal(PartitionAssignmentStrategy.CooperativeSticky, config.PartitionAssignmentStrategy);
        Assert.False(config.EnableAutoCommit);
        Assert.Equal(45000, config.SessionTimeoutMs);
    }

    [Fact]
    public void NumericGetter_NonNumericRawValue_ReturnsNull()
    {
        var config = new ConsumerConfig { ["heartbeat.interval.ms"] = "fast" };
        Assert.Null(config.HeartbeatIntervalMs);
    }

    [Fact]
    public void BooleanGetter_NonBooleanRawValue_ReturnsNull()
    {
        var config = new ConsumerConfig { ["enable.auto.commit"] = "yes" };
        Assert.Null(config.EnableAutoCommit);
    }
}

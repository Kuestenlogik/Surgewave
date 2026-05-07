namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Config;

public class ConsumerConfigTests
{
    [Fact]
    public void GroupId_CanBeSetAndRead()
    {
        var config = new ConsumerConfig
        {
            GroupId = "my-group"
        };

        Assert.Equal("my-group", config.GroupId);
    }

    [Fact]
    public void AutoOffsetReset_DefaultsToNull()
    {
        var config = new ConsumerConfig();
        Assert.Null(config.AutoOffsetReset);
    }

    [Fact]
    public void AutoOffsetReset_CanBeSet()
    {
        var config = new ConsumerConfig { AutoOffsetReset = AutoOffsetReset.Earliest };
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
    }

    [Fact]
    public void EnableAutoCommit_DefaultsToNull()
    {
        var config = new ConsumerConfig();
        Assert.Null(config.EnableAutoCommit);
    }

    [Fact]
    public void EnableAutoCommit_CanBeSet()
    {
        var config = new ConsumerConfig { EnableAutoCommit = false };
        Assert.False(config.EnableAutoCommit);
    }

    [Fact]
    public void AutoCommitIntervalMs_CanBeSet()
    {
        var config = new ConsumerConfig { AutoCommitIntervalMs = 10000 };
        Assert.Equal(10000, config.AutoCommitIntervalMs);
    }

    [Fact]
    public void SessionTimeoutMs_CanBeSet()
    {
        var config = new ConsumerConfig { SessionTimeoutMs = 30000 };
        Assert.Equal(30000, config.SessionTimeoutMs);
    }

    [Fact]
    public void MaxPollIntervalMs_CanBeSet()
    {
        var config = new ConsumerConfig { MaxPollIntervalMs = 600000 };
        Assert.Equal(600000, config.MaxPollIntervalMs);
    }

    [Fact]
    public void IsolationLevel_CanBeSet()
    {
        var config = new ConsumerConfig { IsolationLevel = IsolationLevel.ReadCommitted };
        Assert.Equal(IsolationLevel.ReadCommitted, config.IsolationLevel);
    }

    [Fact]
    public void SurgewaveProtocol_InheritedFromClientConfig()
    {
        var config = new ConsumerConfig { SurgewaveProtocol = "kafka" };
        Assert.Equal("kafka", config.SurgewaveProtocol);
    }

    [Fact]
    public void BootstrapServers_InheritedFromClientConfig()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "broker1:9092,broker2:9092"
        };

        Assert.Equal("broker1:9092,broker2:9092", config.BootstrapServers);
    }
}

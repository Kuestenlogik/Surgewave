using Kuestenlogik.Surgewave.Core.Monitoring;
using Kuestenlogik.Surgewave.Core.Telemetry;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for monitoring and telemetry models: ConsumerGroupLagInfo, LagSummary, LagAlertConfig, TelemetryConfig.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class MonitoringModelTests
{
    #region PartitionLagInfo Tests

    [Fact]
    public void PartitionLagInfo_Properties_SetCorrectly()
    {
        var info = new PartitionLagInfo
        {
            Partition = 3,
            CommittedOffset = 100,
            HighWatermark = 150,
            Lag = 50,
            LogStartOffset = 0,
            AssignedConsumer = "consumer-1"
        };

        Assert.Equal(3, info.Partition);
        Assert.Equal(100, info.CommittedOffset);
        Assert.Equal(150, info.HighWatermark);
        Assert.Equal(50, info.Lag);
        Assert.Equal(0, info.LogStartOffset);
        Assert.Equal("consumer-1", info.AssignedConsumer);
    }

    [Fact]
    public void PartitionLagInfo_NullConsumer_Allowed()
    {
        var info = new PartitionLagInfo
        {
            Partition = 0,
            CommittedOffset = 0,
            HighWatermark = 0,
            Lag = 0,
            LogStartOffset = 0
        };

        Assert.Null(info.AssignedConsumer);
    }

    #endregion

    #region TopicLagInfo Tests

    [Fact]
    public void TopicLagInfo_Properties_SetCorrectly()
    {
        var partitions = new List<PartitionLagInfo>
        {
            new() { Partition = 0, Lag = 10, CommittedOffset = 90, HighWatermark = 100, LogStartOffset = 0 },
            new() { Partition = 1, Lag = 20, CommittedOffset = 80, HighWatermark = 100, LogStartOffset = 0 }
        };

        var info = new TopicLagInfo
        {
            Topic = "orders",
            TotalLag = 30,
            Partitions = partitions
        };

        Assert.Equal("orders", info.Topic);
        Assert.Equal(30, info.TotalLag);
        Assert.Equal(2, info.Partitions.Count);
    }

    #endregion

    #region ConsumerGroupLagInfo Tests

    [Fact]
    public void ConsumerGroupLagInfo_Properties_SetCorrectly()
    {
        var info = new ConsumerGroupLagInfo
        {
            GroupId = "my-group",
            State = "Stable",
            TotalLag = 100,
            PartitionCount = 4,
            MemberCount = 2,
            Topics = new List<TopicLagInfo>()
        };

        Assert.Equal("my-group", info.GroupId);
        Assert.Equal("Stable", info.State);
        Assert.Equal(100, info.TotalLag);
        Assert.Equal(4, info.PartitionCount);
        Assert.Equal(2, info.MemberCount);
        Assert.True(info.Timestamp <= DateTimeOffset.UtcNow);
    }

    #endregion

    #region LagSummary Tests

    [Fact]
    public void LagSummary_Properties_SetCorrectly()
    {
        var summary = new LagSummary
        {
            GroupCount = 3,
            GroupsWithHighLag = 1,
            TotalLag = 5000,
            MaxLag = 3000,
            MaxLagGroup = "slow-group",
            Groups = new List<ConsumerGroupLagInfo>()
        };

        Assert.Equal(3, summary.GroupCount);
        Assert.Equal(1, summary.GroupsWithHighLag);
        Assert.Equal(5000, summary.TotalLag);
        Assert.Equal(3000, summary.MaxLag);
        Assert.Equal("slow-group", summary.MaxLagGroup);
    }

    #endregion

    #region LagAlertConfig Tests

    [Fact]
    public void LagAlertConfig_DefaultValues_AreCorrect()
    {
        var config = new LagAlertConfig();

        Assert.Equal(1000, config.WarningThreshold);
        Assert.Equal(10000, config.CriticalThreshold);
        Assert.Equal(TimeSpan.FromSeconds(30), config.CheckInterval);
        Assert.True(config.Enabled);
    }

    [Fact]
    public void LagAlertConfig_CustomValues_SetCorrectly()
    {
        var config = new LagAlertConfig
        {
            WarningThreshold = 500,
            CriticalThreshold = 5000,
            CheckInterval = TimeSpan.FromMinutes(1),
            Enabled = false
        };

        Assert.Equal(500, config.WarningThreshold);
        Assert.Equal(5000, config.CriticalThreshold);
        Assert.Equal(TimeSpan.FromMinutes(1), config.CheckInterval);
        Assert.False(config.Enabled);
    }

    #endregion

    #region TelemetryConfig Tests

    [Fact]
    public void TelemetryConfig_SectionName_IsCorrect()
    {
        Assert.Equal("Telemetry", TelemetryConfig.SectionName);
    }

    [Fact]
    public void TelemetryConfig_DefaultValues_AreCorrect()
    {
        var config = new TelemetryConfig();

        Assert.Equal("Kuestenlogik.Surgewave", config.ServiceName);
        Assert.Equal("1.0.0", config.ServiceVersion);
        Assert.NotNull(config.Otlp);
        Assert.NotNull(config.Prometheus);
        Assert.NotNull(config.Tracing);
    }

    [Fact]
    public void OtlpConfig_DefaultValues_AreCorrect()
    {
        var config = new OtlpConfig();

        Assert.False(config.Enabled);
        Assert.Equal("http://localhost:4317", config.Endpoint);
        Assert.Equal("Grpc", config.Protocol);
        Assert.Null(config.Headers);
        Assert.Equal(30000, config.TimeoutMs);
    }

    [Fact]
    public void PrometheusConfig_DefaultValues_AreCorrect()
    {
        var config = new PrometheusConfig();

        Assert.True(config.Enabled);
        Assert.Equal("/metrics", config.Path);
    }

    [Fact]
    public void TracingConfig_DefaultValues_AreCorrect()
    {
        var config = new TracingConfig();

        Assert.Equal(1.0, config.SamplingRatio);
        Assert.True(config.IncludeAspNetCore);
        Assert.True(config.IncludeGrpc);
    }

    #endregion
}

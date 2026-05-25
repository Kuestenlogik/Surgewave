using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Audit;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Broker.Telemetry;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Wire-binding contracts for three small admin RPCs:
/// <list type="bullet">
///   <item><c>DescribeLogDirs</c> (35) — projects every topic-partition's
///         <c>TotalSize</c> against the broker's single data directory.</item>
///   <item><c>AlterReplicaLogDirs</c> (34) — Surgewave has no JBOD; every
///         requested partition is rejected with <c>LogDirNotFound</c>.</item>
///   <item><c>ListConfigResources</c> (74, KIP-1106) — returns the
///         configured client-metrics subscriptions when telemetry is on,
///         empty otherwise.</item>
/// </list>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class LogDirsAndListConfigResourcesTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly QuotaManager _quotaManager;
    private readonly TopicAdminHandler _topicAdminHandler;

    public LogDirsAndListConfigResourcesTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-logdirs-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);

        _logManager.CreateTopicAsync("orders", partitionCount: 3).GetAwaiter().GetResult();
        _logManager.CreateTopicAsync("payments", partitionCount: 1).GetAwaiter().GetResult();

        var config = new BrokerConfig { BrokerId = 1 };
        _quotaManager = new QuotaManager(config.Quotas, NullLogger<QuotaManager>.Instance);
        _topicAdminHandler = new TopicAdminHandler(
            config, _logManager, _quotaManager, auditLogger: null,
            NullLogger<TopicAdminHandler>.Instance);
    }

    public void Dispose()
    {
        _quotaManager.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task DescribeLogDirs_NoTopicFilter_ReturnsAllPartitions()
    {
        var resp = (DescribeLogDirsResponse)await _topicAdminHandler.HandleAsync(
            new DescribeLogDirsRequest
            {
                ApiKey = ApiKey.DescribeLogDirs,
                ApiVersion = 4,
                CorrelationId = 1,
                ClientId = "admin",
                Topics = null, // null = all topics
            },
            BuildContext(),
            CancellationToken.None);

        var result = Assert.Single(resp.Results);
        Assert.Equal(ErrorCode.None, result.ErrorCode);
        Assert.Equal(Path.GetFullPath(_dataDir), result.LogDir);
        Assert.Equal(2, result.Topics.Count);
        var orders = result.Topics.Single(t => t.Topic == "orders");
        Assert.Equal(3, orders.Partitions.Count);
    }

    [Fact]
    public async Task DescribeLogDirs_TopicFilter_NarrowsResponse()
    {
        var resp = (DescribeLogDirsResponse)await _topicAdminHandler.HandleAsync(
            new DescribeLogDirsRequest
            {
                ApiKey = ApiKey.DescribeLogDirs,
                ApiVersion = 4,
                CorrelationId = 1,
                ClientId = "admin",
                Topics =
                [
                    new DescribeLogDirsRequest.TopicRequest
                    {
                        Topic = "orders",
                        Partitions = [0, 2],
                    },
                ],
            },
            BuildContext(),
            CancellationToken.None);

        var topic = Assert.Single(Assert.Single(resp.Results).Topics);
        Assert.Equal("orders", topic.Topic);
        Assert.Equal(2, topic.Partitions.Count);
        Assert.Contains(topic.Partitions, p => p.PartitionIndex == 0);
        Assert.Contains(topic.Partitions, p => p.PartitionIndex == 2);
    }

    [Fact]
    public async Task DescribeLogDirs_V4_ReturnsVolumeBytesOrMinusOne()
    {
        var resp = (DescribeLogDirsResponse)await _topicAdminHandler.HandleAsync(
            new DescribeLogDirsRequest
            {
                ApiKey = ApiKey.DescribeLogDirs,
                ApiVersion = 4,
                CorrelationId = 1,
                ClientId = "admin",
                Topics = null,
            },
            BuildContext(),
            CancellationToken.None);

        var result = Assert.Single(resp.Results);
        // Either the volume is readable (positive numbers) or unavailable
        // (-1, -1) — both are valid, the test just pins that we never crash
        // and surface SOMETHING for v4+ clients.
        Assert.True(result.TotalBytes == -1 || result.TotalBytes > 0);
        Assert.True(result.UsableBytes == -1 || result.UsableBytes > 0);
    }

    [Fact]
    public async Task AlterReplicaLogDirs_RejectsEveryPartitionWithLogDirNotFound()
    {
        var resp = (AlterReplicaLogDirsResponse)await _topicAdminHandler.HandleAsync(
            new AlterReplicaLogDirsRequest
            {
                ApiKey = ApiKey.AlterReplicaLogDirs,
                ApiVersion = 2,
                CorrelationId = 1,
                ClientId = "admin",
                Dirs =
                [
                    new AlterReplicaLogDirsRequest.DirEntry
                    {
                        Path = "/some/other/dir",
                        Topics =
                        [
                            new AlterReplicaLogDirsRequest.TopicEntry
                            {
                                Topic = "orders",
                                Partitions = [0, 1, 2],
                            },
                        ],
                    },
                ],
            },
            BuildContext(),
            CancellationToken.None);

        var topic = Assert.Single(resp.Results);
        Assert.Equal("orders", topic.Topic);
        Assert.Equal(3, topic.Partitions.Count);
        Assert.All(topic.Partitions, p => Assert.Equal(ErrorCode.LogDirNotFound, p.ErrorCode));
    }

    [Fact]
    public async Task ListConfigResources_TelemetryDisabled_ReturnsEmptyList()
    {
        var handler = BuildTelemetryHandler(enabled: false);

        var resp = (ListConfigResourcesResponse)await handler.HandleAsync(
            new ListConfigResourcesRequest
            {
                ApiKey = ApiKey.ListConfigResources,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
            },
            BuildContext(),
            CancellationToken.None);

        Assert.Equal(ErrorCode.None, resp.ErrorCode);
        Assert.Empty(resp.ConfigResources);
    }

    [Fact]
    public async Task ListConfigResources_TelemetryEnabled_ReturnsConfiguredMetricNames()
    {
        var handler = BuildTelemetryHandler(
            enabled: true,
            requestedMetrics: ["org.apache.kafka.producer.*", "org.apache.kafka.consumer.*"]);

        var resp = (ListConfigResourcesResponse)await handler.HandleAsync(
            new ListConfigResourcesRequest
            {
                ApiKey = ApiKey.ListConfigResources,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
            },
            BuildContext(),
            CancellationToken.None);

        Assert.Equal(2, resp.ConfigResources.Count);
        Assert.Contains(resp.ConfigResources, r => r.Name == "org.apache.kafka.producer.*");
        Assert.Contains(resp.ConfigResources, r => r.Name == "org.apache.kafka.consumer.*");
    }

    private static TelemetryApiHandler BuildTelemetryHandler(bool enabled, List<string>? requestedMetrics = null)
    {
        var config = new ClientTelemetryConfig
        {
            Enabled = enabled,
            RequestedMetrics = requestedMetrics ?? [],
        };
        var ingestor = new RecordingIngestor();
        return new TelemetryApiHandler(NullLogger<TelemetryApiHandler>.Instance, config, ingestor);
    }

    private static RequestContext BuildContext() => new()
    {
        ConnectionState = new ConnectionState("test-host"),
        ClientId = "admin",
    };

    private sealed class RecordingIngestor : ITelemetryIngestor
    {
        public ValueTask IngestAsync(TelemetryPushEvent push, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }
}

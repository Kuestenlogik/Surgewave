using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-966 DescribeTopicPartitions (API 75): pin the pagination contract.
/// The Kafka Java client 4.0+ uses this API to walk large clusters'
/// metadata in bounded chunks; getting the cursor / limit semantics wrong
/// would manifest as "lost topics on big clusters" with no precise
/// failure mode for the Java side to surface.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DescribeTopicPartitionsTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly MetadataApiHandler _handler;

    public DescribeTopicPartitionsTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-dtp-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);

        // Seed a deterministic topic / partition layout for paging tests.
        _logManager.CreateTopicAsync("orders", partitionCount: 4).GetAwaiter().GetResult();
        _logManager.CreateTopicAsync("payments", partitionCount: 2).GetAwaiter().GetResult();

        var config = new BrokerConfig { BrokerId = 1, Host = "localhost", Port = 9092 };
        _handler = new MetadataApiHandler(config, _logManager, NullLogger<MetadataApiHandler>.Instance);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task SingleTopic_NoCursor_ReturnsAllPartitions()
    {
        var resp = (DescribeTopicPartitionsResponse)await _handler.HandleAsync(
            new DescribeTopicPartitionsRequest
            {
                ApiKey = ApiKey.DescribeTopicPartitions,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                Topics = [new DescribeTopicPartitionsRequest.TopicRequest { Name = "orders" }],
            },
            BuildContext(),
            CancellationToken.None);

        var topic = Assert.Single(resp.Topics);
        Assert.Equal("orders", topic.Name);
        Assert.Equal(4, topic.Partitions.Count);
        Assert.Null(resp.NextCursor);
        Assert.Equal(ErrorCode.None, topic.ErrorCode);
        Assert.NotEqual(Guid.Empty, topic.TopicId);
    }

    [Fact]
    public async Task UnknownTopic_PreservesRowWithError()
    {
        var resp = (DescribeTopicPartitionsResponse)await _handler.HandleAsync(
            new DescribeTopicPartitionsRequest
            {
                ApiKey = ApiKey.DescribeTopicPartitions,
                ApiVersion = 0,
                CorrelationId = 2,
                ClientId = "admin",
                Topics = [new DescribeTopicPartitionsRequest.TopicRequest { Name = "ghost" }],
            },
            BuildContext(),
            CancellationToken.None);

        var topic = Assert.Single(resp.Topics);
        Assert.Equal("ghost", topic.Name);
        Assert.Equal(ErrorCode.UnknownTopicOrPartition, topic.ErrorCode);
        Assert.Empty(topic.Partitions);
    }

    [Fact]
    public async Task ResponsePartitionLimit_TruncatesAndEmitsCursor()
    {
        // Limit 3 → first 3 of orders' 4 partitions; NextCursor points at orders/3.
        var resp = (DescribeTopicPartitionsResponse)await _handler.HandleAsync(
            new DescribeTopicPartitionsRequest
            {
                ApiKey = ApiKey.DescribeTopicPartitions,
                ApiVersion = 0,
                CorrelationId = 3,
                ClientId = "admin",
                Topics =
                [
                    new DescribeTopicPartitionsRequest.TopicRequest { Name = "orders" },
                    new DescribeTopicPartitionsRequest.TopicRequest { Name = "payments" },
                ],
                ResponsePartitionLimit = 3,
            },
            BuildContext(),
            CancellationToken.None);

        Assert.NotNull(resp.NextCursor);
        Assert.Equal("orders", resp.NextCursor!.TopicName);
        Assert.Equal(3, resp.NextCursor.PartitionIndex);
        var topic = Assert.Single(resp.Topics);
        Assert.Equal("orders", topic.Name);
        Assert.Equal(3, topic.Partitions.Count);
    }

    [Fact]
    public async Task StartingCursor_ResumesFromMidTopic()
    {
        // Resume the previous test's response: cursor at orders/3 → emit
        // orders/3 then payments/0..1 within a 10-partition limit.
        var resp = (DescribeTopicPartitionsResponse)await _handler.HandleAsync(
            new DescribeTopicPartitionsRequest
            {
                ApiKey = ApiKey.DescribeTopicPartitions,
                ApiVersion = 0,
                CorrelationId = 4,
                ClientId = "admin",
                Topics =
                [
                    new DescribeTopicPartitionsRequest.TopicRequest { Name = "orders" },
                    new DescribeTopicPartitionsRequest.TopicRequest { Name = "payments" },
                ],
                ResponsePartitionLimit = 10,
                StartingCursor = new DescribeTopicPartitionsRequest.Cursor
                {
                    TopicName = "orders",
                    PartitionIndex = 3,
                },
            },
            BuildContext(),
            CancellationToken.None);

        Assert.Null(resp.NextCursor); // everything fit
        Assert.Equal(2, resp.Topics.Count);
        var ordersTail = resp.Topics.Single(t => t.Name == "orders");
        var paymentsAll = resp.Topics.Single(t => t.Name == "payments");
        Assert.Single(ordersTail.Partitions);
        Assert.Equal(3, ordersTail.Partitions[0].PartitionIndex);
        Assert.Equal(2, paymentsAll.Partitions.Count);
    }

    [Fact]
    public async Task InternalTopic_FlaggedAsInternal()
    {
        await _logManager.CreateTopicAsync("__cluster_metadata", partitionCount: 1);

        var resp = (DescribeTopicPartitionsResponse)await _handler.HandleAsync(
            new DescribeTopicPartitionsRequest
            {
                ApiKey = ApiKey.DescribeTopicPartitions,
                ApiVersion = 0,
                CorrelationId = 5,
                ClientId = "admin",
                Topics = [new DescribeTopicPartitionsRequest.TopicRequest { Name = "__cluster_metadata" }],
            },
            BuildContext(),
            CancellationToken.None);

        Assert.True(Assert.Single(resp.Topics).IsInternal);
    }

    private static RequestContext BuildContext() => new()
    {
        ConnectionState = new ConnectionState("test-host"),
        ClientId = "admin",
    };
}

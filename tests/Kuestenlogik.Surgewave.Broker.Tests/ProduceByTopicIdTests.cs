using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Produce v13 names the topic by its id only (KIP-516). The handler took the absent name for "", so every
/// record a Java 4.x producer sent landed in a topic with an empty name while the producer was told success,
/// and the real topic stayed empty.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ProduceByTopicIdTests : IAsyncDisposable
{
    private const string Topic = "orders";

    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _stateStore;
    private readonly TransactionCoordinator _coordinator;
    private readonly DataApiHandler _handler;

    public ProduceByTopicIdTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-produce-id-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _offsetStore = new OffsetStore(_dataDir, NullLogger<OffsetStore>.Instance);
        _stateStore = new TransactionStateStore(Path.Combine(_dataDir, "txn-state"), NullLogger<TransactionStateStore>.Instance);
        _coordinator = new TransactionCoordinator(
            new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore, _stateStore,
            NullLogger<TransactionCoordinator>.Instance);
        var config = new BrokerConfig();
        _handler = new DataApiHandler(
            config, _logManager, _coordinator,
            new QuotaManager(config.Quotas, NullLogger<QuotaManager>.Instance),
            new RecordBatchSerializer(NullLogger<RecordBatchSerializer>.Instance),
            aclAuthorizer: null, deduplicationManager: null, delayIndex: null, ttlIndex: null,
            metrics: null, NullLogger<DataApiHandler>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        await _coordinator.DisposeAsync();
        _offsetStore.Dispose();
        _stateStore.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task V13_WritesToTheTopicItsIdNames()
    {
        var metadata = await _logManager.CreateTopicAsync(Topic, partitionCount: 1);

        var response = await ProduceAsync(metadata.TopicId);

        var partition = response.Responses[0].PartitionResponses[0];
        Assert.Equal(ErrorCode.None, partition.ErrorCode);
        Assert.Equal(1, _logManager.GetLog(new TopicPartition { Topic = Topic, Partition = 0 })!.NextOffset);
        Assert.Null(_logManager.GetLog(new TopicPartition { Topic = string.Empty, Partition = 0 }));
    }

    [Fact]
    public async Task V13_WithAnUnknownTopicId_IsUnknownTopicIdAndWritesNothing()
    {
        var response = await ProduceAsync(Guid.NewGuid());

        Assert.Equal(ErrorCode.UnknownTopicId, response.Responses[0].PartitionResponses[0].ErrorCode);
        Assert.Null(_logManager.GetLog(new TopicPartition { Topic = string.Empty, Partition = 0 }));
    }

    private async Task<ProduceResponse> ProduceAsync(Guid topicId)
    {
        var request = new ProduceRequest
        {
            ApiKey = ApiKey.Produce,
            ApiVersion = 13,
            CorrelationId = 1,
            ClientId = "java-producer",
            RequiredAcks = 1,
            TimeoutMs = 30_000,
            TopicData =
            [
                new ProduceRequest.TopicProduceData
                {
                    Name = null,
                    TopicId = topicId,
                    PartitionData = [new ProduceRequest.PartitionProduceData { Index = 0, Records = CreateBatch() }]
                }
            ]
        };

        var response = await _handler.HandleAsync(
            request,
            new RequestContext { ConnectionState = new ConnectionState("java-producer"), ClientId = "java-producer" },
            CancellationToken.None);
        return Assert.IsType<ProduceResponse>(response);
    }

    private static ReadOnlyMemory<byte> CreateBatch()
    {
        const int size = 100;
        var batch = new byte[size];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), size - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);
        batch[16] = 2;
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), 0);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);   // no producer id
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);
        return batch;
    }
}

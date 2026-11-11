using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Coordination.Transactions;
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
/// A broker whose log directory has failed stops serving the partitions in it (#162): produce and fetch answer
/// KAFKA_STORAGE_ERROR, as Kafka's broker does, which a client retries after refreshing its metadata. A fetch at the
/// high watermark must answer too, or the consumer would wait on this broker for good.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class KafkaOfflineLogDirTests : IDisposable
{
    private const string Topic = "offline-dir";

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-offline-dir-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore = OffsetStore.InMemory();
    private readonly DataApiHandler _handler;

    public KafkaOfflineLogDirTests()
    {
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        var config = new BrokerConfig();
        var transactionCoordinator = new TransactionCoordinator(
            new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore,
            Kuestenlogik.Surgewave.Broker.Transactions.TransactionStateLog.InMemory(),
            NullLogger<TransactionCoordinator>.Instance);
        _handler = new DataApiHandler(
            config,
            _logManager,
            transactionCoordinator,
            new QuotaManager(config.Quotas, NullLogger<QuotaManager>.Instance),
            new RecordBatchSerializer(NullLogger<RecordBatchSerializer>.Instance),
            aclAuthorizer: null,
            deduplicationManager: null,
            delayIndex: null,
            ttlIndex: null,
            metrics: null,
            NullLogger<DataApiHandler>.Instance);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        _offsetStore.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    private async Task<TopicPartition> TopicWithOneRecordAsync()
    {
        await _logManager.CreateTopicAsync(Topic, 1);
        var tp = new TopicPartition { Topic = Topic, Partition = 0 };
        Assert.Equal(ErrorCode.None, (await ProduceAsync()).ErrorCode);
        return tp;
    }

    [Fact]
    public async Task ProduceToAPartitionInAnOfflineDirectoryAnswersKafkaStorageError()
    {
        var tp = await TopicWithOneRecordAsync();
        _logManager.LogDirectory.Fail("append to", tp, new IOException("disk gone"));

        var response = await ProduceAsync();

        Assert.Equal(ErrorCode.KafkaStorageError, response.ErrorCode);
        Assert.Equal(-1, response.BaseOffset);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)] // the high watermark: nothing to read, so nothing would fail
    public async Task FetchFromAPartitionInAnOfflineDirectoryAnswersKafkaStorageError(long fetchOffset)
    {
        var tp = await TopicWithOneRecordAsync();
        _logManager.LogDirectory.Fail("append to", tp, new IOException("disk gone"));

        var response = await _handler.HandleAsync(
            new FetchRequest
            {
                ApiKey = ApiKey.Fetch,
                ApiVersion = 11,
                CorrelationId = 2,
                ClientId = "offline-test",
                ReplicaId = -1,
                MaxWaitMs = 0,
                MinBytes = 1,
                MaxBytes = 1024 * 1024,
                Topics =
                [
                    new FetchRequest.FetchTopic
                    {
                        Topic = Topic,
                        Partitions = [new FetchRequest.FetchPartition { Partition = 0, FetchOffset = fetchOffset, MaxBytes = 1024 * 1024 }]
                    }
                ]
            },
            new RequestContext { ConnectionState = new ConnectionState("offline-test"), ClientId = "offline-test" },
            TestContext.Current.CancellationToken);

        var fetch = Assert.IsType<FetchResponse>(response);
        Assert.Equal(ErrorCode.KafkaStorageError, fetch.Responses[0].Partitions[0].ErrorCode);
    }

    private async Task<ProduceResponse.PartitionProduceResponse> ProduceAsync()
    {
        var response = await _handler.HandleAsync(
            new ProduceRequest
            {
                ApiKey = ApiKey.Produce,
                ApiVersion = 9,
                CorrelationId = 1,
                ClientId = "offline-test",
                RequiredAcks = 1,
                TimeoutMs = 30_000,
                TopicData =
                [
                    new ProduceRequest.TopicProduceData
                    {
                        Name = Topic,
                        PartitionData = [new ProduceRequest.PartitionProduceData { Index = 0, Records = Batch() }]
                    }
                ]
            },
            new RequestContext { ConnectionState = new ConnectionState("offline-test"), ClientId = "offline-test" },
            TestContext.Current.CancellationToken);
        return Assert.IsType<ProduceResponse>(response).Responses[0].PartitionResponses[0];
    }

    private static ReadOnlyMemory<byte> Batch()
    {
        const int size = 100;
        var batch = new byte[size];
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), size - 12);
        batch[16] = 2;
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);                // no producer id
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), Crc32C.Compute(batch.AsSpan(21)));
        return batch;
    }
}

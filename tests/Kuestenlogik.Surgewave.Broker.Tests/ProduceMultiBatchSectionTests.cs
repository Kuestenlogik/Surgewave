using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
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
/// A produce request's records field may carry several record batches for one partition — the Kafka
/// protocol concatenates them, exactly as the replication fetch response does.
///
/// <para>On the replication path, appending such a section as one blob was #92/#93: the receiver
/// ended up with a single batch carrying one CRC over the concatenation. The produce path has the
/// same shape — <c>RecordHeaderParser.ParseBatchHeader</c> reads only the FIRST header before the
/// whole section is handed to a single append — so the question is whether it has the same defect.
/// It does not: the validating append (#85) computes the CRC over the entire section, that cannot
/// match the first batch's CRC field, and the write is refused.</para>
///
/// <para><b>What these tests pin is the safety property, not the ideal.</b> Nothing is merged, no
/// batch is stored with a CRC describing other batches' bytes, and nothing is half-written. The
/// broker refusing a legal multi-batch section is a separate, tracked gap — a producer that sends
/// one is told its data is corrupt when it is not. Should that gap be closed, these tests must be
/// rewritten to assert three stored batches; they must NOT be relaxed into accepting a merge.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ProduceMultiBatchSectionTests : IDisposable
{
    private const string Topic = "multi-batch-topic";

    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _transactionStateStore;
    private readonly DataApiHandler _handler;

    public ProduceMultiBatchSectionTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-multibatch-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);

        var config = new BrokerConfig();
        _offsetStore = new OffsetStore(_dataDir, NullLogger<OffsetStore>.Instance);
        _transactionStateStore = new TransactionStateStore(_dataDir, NullLogger<TransactionStateStore>.Instance);
        var transactionCoordinator = new TransactionCoordinator(
            new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore, _transactionStateStore,
            NullLogger<TransactionCoordinator>.Instance);

        _handler = new DataApiHandler(
            config, _logManager, transactionCoordinator,
            new QuotaManager(config.Quotas, NullLogger<QuotaManager>.Instance),
            new RecordBatchSerializer(NullLogger<RecordBatchSerializer>.Instance),
            aclAuthorizer: null, deduplicationManager: null, delayIndex: null, ttlIndex: null,
            metrics: null, NullLogger<DataApiHandler>.Instance);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        _offsetStore.Dispose();
        _transactionStateStore.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task MultiBatchSection_IsRefusedRatherThanMergedIntoOneBatch()
    {
        var section = Concat(
            CreateValidBatch(baseOffset: 0, recordCount: 1),
            CreateValidBatch(baseOffset: 1, recordCount: 1),
            CreateValidBatch(baseOffset: 2, recordCount: 1));

        var response = await ProduceAsync(section);

        // The #92 outcome would be ErrorCode.None plus one stored batch whose CRC covers all three.
        Assert.Equal(ErrorCode.CorruptMessage, response.Responses[0].PartitionResponses[0].ErrorCode);
    }

    [Fact]
    public async Task MultiBatchSection_LeavesNothingBehindInTheLog()
    {
        var section = Concat(
            CreateValidBatch(baseOffset: 0, recordCount: 1),
            CreateValidBatch(baseOffset: 1, recordCount: 1),
            CreateValidBatch(baseOffset: 2, recordCount: 1));

        await ProduceAsync(section);

        // Not a partial write and not a merged one: the refusal is clean.
        var tp = new TopicPartition { Topic = Topic, Partition = 0 };
        Assert.Equal(0, _logManager.GetLog(tp)?.NextOffset ?? 0);
    }

    [Fact]
    public async Task SingleBatchSection_IsStoredIntact()
    {
        // The control: one batch per partition per request — what the Java producer and librdkafka
        // actually send — goes through untouched, so the refusal above is about the section shape
        // and not about this test's batch format.
        var response = await ProduceAsync(CreateValidBatch(baseOffset: 0, recordCount: 3));

        Assert.Equal(ErrorCode.None, response.Responses[0].PartitionResponses[0].ErrorCode);

        var tp = new TopicPartition { Topic = Topic, Partition = 0 };
        var stored = await _logManager.ReadBatchesAsync(tp, 0, maxBytes: 1024 * 1024);
        Assert.Single(stored);
        Assert.True(RecordBatchValidator.ValidateCrc(stored[0]));
    }

    private async Task<ProduceResponse> ProduceAsync(ReadOnlyMemory<byte> records)
    {
        var request = new ProduceRequest
        {
            ApiKey = ApiKey.Produce,
            ApiVersion = 9,
            CorrelationId = 1,
            ClientId = "multi-batch",
            RequiredAcks = 1,
            TimeoutMs = 30_000,
            TopicData =
            [
                new ProduceRequest.TopicProduceData
                {
                    Name = Topic,
                    PartitionData = [new ProduceRequest.PartitionProduceData { Index = 0, Records = records }]
                }
            ]
        };

        var response = await _handler.HandleAsync(
            request,
            new RequestContext { ConnectionState = new ConnectionState("multi-batch"), ClientId = "multi-batch" },
            CancellationToken.None);
        return Assert.IsType<ProduceResponse>(response);
    }

    private static ReadOnlyMemory<byte> Concat(params byte[][] batches)
    {
        var total = batches.Sum(b => b.Length);
        var buffer = new byte[total];
        var offset = 0;
        foreach (var batch in batches)
        {
            batch.CopyTo(buffer, offset);
            offset += batch.Length;
        }
        return buffer;
    }

    private static byte[] CreateValidBatch(long baseOffset, int recordCount)
    {
        const int size = 100;
        var batch = new byte[size];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), baseOffset);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), size - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);
        batch[16] = 2;
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), recordCount - 1);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), recordCount);
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);
        return batch;
    }
}

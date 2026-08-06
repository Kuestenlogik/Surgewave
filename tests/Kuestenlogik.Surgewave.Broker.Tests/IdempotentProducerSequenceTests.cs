using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Replication;
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
/// An idempotent producer numbers RECORDS and expects a retransmit to be answered with the offset
/// its batch originally landed at (#122).
///
/// <para>Two things were wrong. The broker stored a batch's BASE sequence and then expected the next
/// batch to start one higher, so any producer sending more than one record per batch was told its
/// second batch was out of order. And a retransmit — the ordinary consequence of an acknowledgement
/// that never arrived — came back as <c>DuplicateSequenceNumber</c> with offset -1, which is fatal
/// in both the Java producer and librdkafka, instead of as success with the original offset.</para>
///
/// <para>Both matter for acks=all specifically: <c>enable.idempotence</c> requires it, so the first
/// user of durable produce is an idempotent producer, and a durability refusal or a commit timeout
/// is exactly the situation that triggers a retry.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class IdempotentProducerSequenceTests : IDisposable
{
    private const string Topic = "idempotence-topic";
    private const long ProducerId = 9001;

    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _transactionStateStore;
    private readonly DataApiHandler _handler;
    private readonly StubCommitGate _gate = new();

    public IdempotentProducerSequenceTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-idem-" + Guid.NewGuid().ToString("N"));
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
        _handler.SetCommitGate(_gate);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        _offsetStore.Dispose();
        _transactionStateStore.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task MultiRecordBatches_ContinueWhereTheRecordsLeftOff()
    {
        // Three records per batch: the second batch starts at sequence 3, not 1. Expecting 1 is what
        // made every real producer's second batch fail — batches of one record are the exception.
        var first = await ProduceAsync(baseSequence: 0, recordCount: 3);
        Assert.Equal(ErrorCode.None, First(first).ErrorCode);
        Assert.Equal(0, First(first).BaseOffset);

        var second = await ProduceAsync(baseSequence: 3, recordCount: 3);

        Assert.Equal(ErrorCode.None, First(second).ErrorCode);
        Assert.Equal(3, First(second).BaseOffset);
    }

    [Fact]
    public async Task ARetransmittedBatch_IsAnsweredWithItsOriginalOffset()
    {
        await ProduceAsync(baseSequence: 0, recordCount: 2);
        var second = await ProduceAsync(baseSequence: 2, recordCount: 2);
        Assert.Equal(2, First(second).BaseOffset);

        // The producer never saw that acknowledgement and sends the same batch again.
        var retransmit = await ProduceAsync(baseSequence: 2, recordCount: 2);

        Assert.Equal(ErrorCode.None, First(retransmit).ErrorCode);
        Assert.Equal(2, First(retransmit).BaseOffset);

        // And it was not written a second time.
        var tp = new TopicPartition { Topic = Topic, Partition = 0 };
        Assert.Equal(4, _logManager.GetLog(tp)!.NextOffset);
    }

    [Fact]
    public async Task ARefusedBatch_DoesNotConsumeItsSequence()
    {
        // The reason validation no longer mutates: something between validation and the append can
        // still refuse the write. Here it is the durability gate, but a quota or a corrupt payload
        // would do the same.
        await ProduceAsync(baseSequence: 0, recordCount: 2);

        _gate.Admit = false;
        var refused = await ProduceAsync(baseSequence: 2, recordCount: 2, acks: -1);
        Assert.Equal(ErrorCode.NotEnoughReplicas, First(refused).ErrorCode);

        _gate.Admit = true;
        var retried = await ProduceAsync(baseSequence: 2, recordCount: 2);

        Assert.Equal(ErrorCode.None, First(retried).ErrorCode);
        Assert.Equal(2, First(retried).BaseOffset);
    }

    [Fact]
    public async Task AGenuinelyOutOfOrderBatch_IsStillRejected()
    {
        // The permissive answers above must not turn into "accept anything": a gap in the sequence
        // is still an error, because accepting it would silently lose records.
        await ProduceAsync(baseSequence: 0, recordCount: 2);

        var gap = await ProduceAsync(baseSequence: 7, recordCount: 2);

        Assert.Equal(ErrorCode.OutOfOrderSequenceNumber, First(gap).ErrorCode);
        Assert.Equal(-1, First(gap).BaseOffset);
    }

    private async Task<ProduceResponse> ProduceAsync(int baseSequence, int recordCount, short acks = 1)
    {
        var request = new ProduceRequest
        {
            ApiKey = ApiKey.Produce,
            ApiVersion = 9,
            CorrelationId = 1,
            ClientId = "idem-test",
            RequiredAcks = acks,
            TimeoutMs = 30_000,
            TopicData =
            [
                new ProduceRequest.TopicProduceData
                {
                    Name = Topic,
                    PartitionData =
                    [
                        new ProduceRequest.PartitionProduceData
                        {
                            Index = 0,
                            Records = CreateBatch(baseSequence, recordCount)
                        }
                    ]
                }
            ]
        };

        var response = await _handler.HandleAsync(
            request,
            new RequestContext { ConnectionState = new ConnectionState("idem-test"), ClientId = "idem-test" },
            CancellationToken.None);
        return Assert.IsType<ProduceResponse>(response);
    }

    private static ProduceResponse.PartitionProduceResponse First(ProduceResponse response)
        => response.Responses[0].PartitionResponses[0];

    private static ReadOnlyMemory<byte> CreateBatch(int baseSequence, int recordCount)
    {
        const int size = 100;
        var batch = new byte[size];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), size - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);
        batch[16] = 2;
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), recordCount - 1); // last offset delta
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), ProducerId);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), 0);                // producer epoch
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), baseSequence);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), recordCount);
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);
        return batch;
    }

    private sealed class StubCommitGate : IPartitionCommitGate
    {
        public bool Admit { get; set; } = true;
        public bool CanAdmitDurableWrite(in TopicPartition partition) => Admit;
    }
}

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
/// A producer that asks for acks=all is told the truth (#122, admission half).
///
/// <para>The broker decoded <c>RequiredAcks</c> and then ignored it: every produce was answered as
/// soon as the leader's own append returned, whatever the client had asked for and whatever the
/// in-sync replica set looked like. That is not a stall but silent under-replication reported as
/// success — the more dangerous shape, because the client sees no reason to retry.</para>
///
/// <para>This step refuses what cannot be honoured. Waiting for the commit is the next one; these
/// tests pin that acks=0/1 are untouched, that a partition short of in-sync replicas refuses the
/// write instead of taking it, and — the part that is easy to get wrong — that a refusal leaves the
/// idempotent producer's sequence exactly where it was.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ProduceDurabilityAdmissionTests : IDisposable
{
    private const string Topic = "durability-topic";

    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _transactionStateStore;
    private readonly DataApiHandler _handler;
    private readonly StubCommitGate _gate = new();

    public ProduceDurabilityAdmissionTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-durability-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);

        var config = new BrokerConfig();
        _offsetStore = new OffsetStore(_dataDir, NullLogger<OffsetStore>.Instance);
        _transactionStateStore = new TransactionStateStore(_dataDir, NullLogger<TransactionStateStore>.Instance);
        var transactionCoordinator = new TransactionCoordinator(
            new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore, _transactionStateStore,
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
    public async Task AcksAll_UnderMinIsr_RefusesTheWriteAndAppendsNothing()
    {
        _gate.Admit = false;

        var response = await ProduceAsync(acks: -1);

        Assert.Equal(ErrorCode.NotEnoughReplicas, FirstPartition(response).ErrorCode);
        Assert.Equal(-1, FirstPartition(response).BaseOffset);
        Assert.Null(_logManager.GetLog(new TopicPartition { Topic = Topic, Partition = 0 }));
    }

    [Fact]
    public async Task AcksAll_WithEnoughReplicas_IsAccepted()
    {
        _gate.Admit = true;

        var response = await ProduceAsync(acks: -1);

        Assert.Equal(ErrorCode.None, FirstPartition(response).ErrorCode);
        Assert.Equal(0, FirstPartition(response).BaseOffset);
    }

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)1)]
    public async Task WeakerAcks_AreNotSubjectToTheGate(short acks)
    {
        // The gate is asked only for acks=-1. A producer that asked for leader-only durability gets
        // exactly what it asked for, and the gate is not even consulted.
        _gate.Admit = false;

        var response = await ProduceAsync(acks);

        Assert.Equal(ErrorCode.None, FirstPartition(response).ErrorCode);
        Assert.Equal(0, _gate.Calls);
    }

    [Theory]
    [InlineData((short)2)]
    [InlineData((short)-2)]
    [InlineData(short.MaxValue)]
    public async Task AcksOutsideTheProtocol_IsRejectedAndAppendsNothing(short acks)
    {
        var response = await ProduceAsync(acks);

        Assert.Equal(ErrorCode.InvalidRequiredAcks, FirstPartition(response).ErrorCode);
        Assert.Null(_logManager.GetLog(new TopicPartition { Topic = Topic, Partition = 0 }));
    }

    [Fact]
    public async Task AcksAll_Refused_DoesNotAdvanceTheIdempotentSequence()
    {
        // The trap this placement exists for. Sequence validation MUTATES the producer's last-seen
        // sequence, so refusing a write after validating it would make the client's retry — which
        // carries the same sequence — come back as DuplicateSequenceNumber, which is not retriable
        // in either common client. And since enable.idempotence requires acks=all, the first user
        // of this feature is an idempotent producer.
        const long producerId = 4711;
        const int baseSequence = 0;

        _gate.Admit = false;
        var refused = await ProduceAsync(acks: -1, producerId, baseSequence);
        Assert.Equal(ErrorCode.NotEnoughReplicas, FirstPartition(refused).ErrorCode);

        // The follower comes back; the producer retries the very same batch.
        _gate.Admit = true;
        var retried = await ProduceAsync(acks: -1, producerId, baseSequence);

        Assert.Equal(ErrorCode.None, FirstPartition(retried).ErrorCode);
        Assert.Equal(0, FirstPartition(retried).BaseOffset);
    }

    [Fact]
    public async Task TheBenchmarkBatchShape_IsAcceptedByTheProducePath()
    {
        // KafkaProduceBenchmarks builds its batch exactly this way — a full v2 header plus an 8 KiB
        // random payload, CRC computed last over everything from the attributes onward. The first
        // version of that benchmark left the CRC field zero and measured the resulting throw instead
        // of the produce path, which is invisible in BenchmarkDotNet's output beyond a "?" column.
        // Pinning the shape here keeps that from happening again without paying for a BDN run.
        const int payloadBytes = 8 * 1024;
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + payloadBytes];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        batch[16] = 2;
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        Random.Shared.NextBytes(batch.AsSpan(KafkaConstants.RecordBatch.HeaderSize));
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);

        var response = await ProduceRawAsync(acks: 1, batch);

        Assert.Equal(ErrorCode.None, FirstPartition(response).ErrorCode);
    }

    private Task<ProduceResponse> ProduceAsync(
        short acks, long producerId = -1, int baseSequence = -1)
        => ProduceRawAsync(acks, CreateBatch(producerId, baseSequence));

    private async Task<ProduceResponse> ProduceRawAsync(short acks, ReadOnlyMemory<byte> records)
    {
        var request = new ProduceRequest
        {
            ApiKey = ApiKey.Produce,
            CorrelationId = 1,
            ApiVersion = 9,
            ClientId = "durability-test",
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
                            Records = records
                        }
                    ]
                }
            ]
        };

        var response = await _handler.HandleAsync(
            request,
            new RequestContext { ConnectionState = new ConnectionState("durability-test"), ClientId = "durability-test" },
            CancellationToken.None);
        return Assert.IsType<ProduceResponse>(response);
    }

    private static ProduceResponse.PartitionProduceResponse FirstPartition(ProduceResponse response)
        => response.Responses[0].PartitionResponses[0];

    private static ReadOnlyMemory<byte> CreateBatch(long producerId, int baseSequence)
    {
        // Minimal single-record v2 batch; only the header fields the broker reads are meaningful.
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
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), producerId);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), baseSequence);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);
        return batch;
    }

    private sealed class StubCommitGate : IPartitionCommitGate
    {
        public bool Admit { get; set; } = true;
        public int Calls { get; private set; }

        public bool CanAdmitDurableWrite(in TopicPartition partition)
        {
            Calls++;
            return Admit;
        }
    }
}

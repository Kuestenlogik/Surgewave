using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Coordination.Transactions;
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

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// A transactional batch is checked against its transaction before it is written (KIP-890, #190). A
/// transactions V2 client (produce v12+, TxnOffsetCommit v5+) sends no AddPartitionsToTxn or AddOffsetsToTxn,
/// so the produce and the offset commit put the partition and the group into the transaction themselves; an
/// older client must have added them. The errors go on the wire per request version, as Kafka's do.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransactionalProduceVerificationTests : IAsyncDisposable
{
    private const string Topic = "txn-topic";
    private const string TxnId = "txn-producer";

    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _stateStore;
    private readonly TransactionCoordinator _coordinator;
    private readonly DataApiHandler _handler;

    public TransactionalProduceVerificationTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-txnverify-" + Guid.NewGuid().ToString("N"));
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

    private async Task<(long Pid, short Epoch)> InitAsync()
    {
        var init = await _coordinator.InitProducerIdAsync(
            new InitProducerIdCommand { TransactionalId = TxnId, TransactionTimeoutMs = 60_000, ProducerId = -1, ProducerEpoch = -1 },
            CancellationToken.None);
        return (init.ProducerId, init.ProducerEpoch);
    }

    private string State() => Assert.Single(_coordinator.DescribeTransactions([TxnId])).State;

    private static readonly TopicPartition Tp = new() { Topic = Topic, Partition = 0 };

    // ── The coordinator's verification ──────────────────────────────────────

    [Fact]
    public async Task V2_TheProduceAddsThePartition()
    {
        var (pid, epoch) = await InitAsync();

        var status = _coordinator.VerifyTransactionalBatch(TxnId, pid, epoch, Tp, TransactionVersion.Tv2);

        Assert.Equal(TxnErrorStatus.None, status);
        Assert.Equal("Ongoing", State());
        Assert.Contains((Topic, 0), Assert.Single(_coordinator.DescribeTransactions([TxnId])).Partitions);
    }

    [Fact]
    public async Task V1_APartitionThatWasNotAdded_IsTransactionAbortable()
    {
        var (pid, epoch) = await InitAsync();

        Assert.Equal(TxnErrorStatus.TransactionAbortable,
            _coordinator.VerifyTransactionalBatch(TxnId, pid, epoch, Tp, TransactionVersion.Tv0));

        _coordinator.AddPartitionsToTxn(new AddPartitionsToTxnCommand
        {
            TransactionalId = TxnId, ProducerId = pid, ProducerEpoch = epoch, Topics = [new AddPartitionsTopic(Topic, [0])],
        });
        Assert.Equal(TxnErrorStatus.None,
            _coordinator.VerifyTransactionalBatch(TxnId, pid, epoch, Tp, TransactionVersion.Tv0));
    }

    [Fact]
    public async Task AnotherEpoch_IsProducerFenced()
    {
        var (pid, epoch) = await InitAsync();

        Assert.Equal(TxnErrorStatus.ProducerFenced,
            _coordinator.VerifyTransactionalBatch(TxnId, pid, (short)(epoch - 1), Tp, TransactionVersion.Tv2));
    }

    [Fact]
    public async Task V2_ACompleteCycle_CommitsTheProducedPartition_AndTheNextTransactionAddsItAgain()
    {
        var (pid, epoch) = await InitAsync();
        _coordinator.VerifyTransactionalBatch(TxnId, pid, epoch, Tp, TransactionVersion.Tv2);

        var first = await _coordinator.EndTxnAsync(
            new EndTxnCommand { TransactionalId = TxnId, ProducerId = pid, ProducerEpoch = epoch, Committed = true, TransactionVersion = TransactionVersion.Tv2 },
            CancellationToken.None);
        Assert.Equal(TxnErrorStatus.None, first.Status);

        // The next transaction runs at the bumped epoch; its first produce starts it.
        Assert.Equal(TxnErrorStatus.None,
            _coordinator.VerifyTransactionalBatch(TxnId, pid, first.ProducerEpoch, Tp, TransactionVersion.Tv2));
        Assert.Equal("Ongoing", State());
        Assert.Equal(TxnErrorStatus.ProducerFenced,
            _coordinator.VerifyTransactionalBatch(TxnId, pid, epoch, Tp, TransactionVersion.Tv2));
    }

    [Fact]
    public async Task V2_TheOffsetCommitAddsTheGroup_AndTheCommitCommitsTheOffset()
    {
        var (pid, epoch) = await InitAsync();

        var staged = _coordinator.TxnOffsetCommit(new TxnOffsetCommitCommand
        {
            TransactionalId = TxnId, GroupId = "g", ProducerId = pid, ProducerEpoch = epoch, TransactionVersion = TransactionVersion.Tv2,
            Topics = [new TxnOffsetCommitTopic { Name = Topic, TopicId = Guid.Empty, Partitions = [new TxnOffsetCommitPartition(0, 17, null)] }],
        });
        Assert.Equal(TxnErrorStatus.None, staged.Topics[0].Partitions[0].Status);
        Assert.Equal("Ongoing", State());

        await _coordinator.EndTxnAsync(
            new EndTxnCommand { TransactionalId = TxnId, ProducerId = pid, ProducerEpoch = epoch, Committed = true, TransactionVersion = TransactionVersion.Tv2 },
            CancellationToken.None);

        Assert.Equal(17, _offsetStore.GetCommittedOffset("g", Topic, 0));
    }

    [Fact]
    public async Task V1_AnOffsetCommitForAGroupThatWasNotAdded_IsTransactionAbortable()
    {
        var (pid, epoch) = await InitAsync();

        var staged = _coordinator.TxnOffsetCommit(new TxnOffsetCommitCommand
        {
            TransactionalId = TxnId, GroupId = "g", ProducerId = pid, ProducerEpoch = epoch,
            Topics = [new TxnOffsetCommitTopic { Name = Topic, TopicId = Guid.Empty, Partitions = [new TxnOffsetCommitPartition(0, 17, null)] }],
        });

        Assert.Equal(TxnErrorStatus.TransactionAbortable, staged.Topics[0].Partitions[0].Status);
    }

    // ── Through the produce handler ─────────────────────────────────────────

    [Fact]
    public async Task ProduceV12_AddsThePartitionAndWrites()
    {
        var (pid, epoch) = await InitAsync();

        var response = await ProduceAsync(version: 12, TxnId, pid, epoch);

        Assert.Equal(ErrorCode.None, First(response).ErrorCode);
        Assert.Equal("Ongoing", State());
    }

    [Theory]
    [InlineData(9, ErrorCode.InvalidTxnState)]
    [InlineData(11, ErrorCode.TransactionAbortable)]
    public async Task OlderProduce_ToAPartitionThatWasNotAdded_IsRefusedAndNothingWritten(short version, ErrorCode expected)
    {
        var (pid, epoch) = await InitAsync();

        var response = await ProduceAsync(version, TxnId, pid, epoch);

        Assert.Equal(expected, First(response).ErrorCode);
        Assert.Equal(0, _logManager.GetLog(Tp)?.NextOffset ?? 0);
    }

    [Fact]
    public async Task Produce_WithAFencedEpoch_IsInvalidProducerEpoch()
    {
        var (pid, epoch) = await InitAsync();

        var response = await ProduceAsync(version: 12, TxnId, pid, (short)(epoch - 1));

        Assert.Equal(ErrorCode.InvalidProducerEpoch, First(response).ErrorCode);
    }

    [Fact]
    public async Task Produce_OfTransactionalRecordsWithoutATransactionalId_IsRefused()
    {
        var (pid, epoch) = await InitAsync();

        var response = await ProduceAsync(version: 12, transactionalId: null, pid, epoch);

        Assert.Equal(ErrorCode.TransactionalIdAuthorizationFailed, First(response).ErrorCode);
    }

    private async Task<ProduceResponse> ProduceAsync(short version, string? transactionalId, long pid, short epoch)
    {
        var request = new ProduceRequest
        {
            ApiKey = ApiKey.Produce,
            ApiVersion = version,
            CorrelationId = 1,
            ClientId = "txn-test",
            TransactionalId = transactionalId,
            RequiredAcks = 1,
            TimeoutMs = 30_000,
            TopicData =
            [
                new ProduceRequest.TopicProduceData
                {
                    Name = Topic,
                    PartitionData = [new ProduceRequest.PartitionProduceData { Index = 0, Records = CreateTransactionalBatch(pid, epoch) }]
                }
            ]
        };

        var response = await _handler.HandleAsync(
            request,
            new RequestContext { ConnectionState = new ConnectionState("txn-test"), ClientId = "txn-test" },
            CancellationToken.None);
        return Assert.IsType<ProduceResponse>(response);
    }

    private static ProduceResponse.PartitionProduceResponse First(ProduceResponse response)
        => response.Responses[0].PartitionResponses[0];

    private static ReadOnlyMemory<byte> CreateTransactionalBatch(long producerId, short epoch)
    {
        const int size = 100;
        var batch = new byte[size];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), size - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);
        batch[16] = 2;
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0x10);                // attributes: transactional
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), 0);                   // last offset delta
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), producerId);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), epoch);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), 0);                   // base sequence
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);                   // record count
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);
        return batch;
    }
}

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
/// A produce request carries exactly ONE record batch per partition, and a section with more is
/// refused as <see cref="ErrorCode.InvalidRecord"/>.
///
/// <para>This file previously recorded the opposite belief — that a multi-batch section is legal
/// Kafka and that refusing it rejects valid client traffic. It is not. Kafka enforces the single
/// batch at parse time: <c>ProduceRequest.validateRecords</c> throws
/// <c>InvalidRecordException("Produce requests with version N are only allowed to contain exactly
/// one record batch per partition")</c> when the iterator still has an element after the first, and
/// its own producer cannot build such a section. Surgewave refusing it is correct behaviour.</para>
///
/// <para>What was wrong was the reason given. The section used to fall through to the append, where
/// the validating CRC (#85) is computed over the whole concatenation and cannot match the first
/// batch's CRC field, so the producer was told <c>CorruptMessage</c> — transport corruption — for a
/// request the protocol simply does not permit. Now the refusal is explicit and carries Kafka's own
/// code.</para>
///
/// <para><b>The safety property still holds and is still what these tests pin:</b> nothing is
/// merged, no batch is stored with a CRC describing other batches' bytes, and nothing is
/// half-written. On the replication path that same shape WAS real corruption (#92/#93); here it
/// never reached the log. Do not relax these tests into accepting a merge, and do not "fix" the
/// refusal by making the broker store multi-batch sections: that would be a deliberate protocol
/// superset reachable only by hand-crafted traffic, on the hottest path in the broker.</para>
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
    public async Task MultiBatchSection_IsRefusedAsInvalidRecord()
    {
        var section = Concat(
            CreateValidBatch(baseOffset: 0, recordCount: 1),
            CreateValidBatch(baseOffset: 1, recordCount: 1),
            CreateValidBatch(baseOffset: 2, recordCount: 1));

        var response = await ProduceAsync(section);

        // Kafka's own code for this, and NOT CorruptMessage: every batch here carries a correct CRC
        // over its own bytes. Blaming transport for a protocol violation is what sent people
        // looking for a network fault that was not there. The #92 outcome — ErrorCode.None plus one
        // stored batch whose CRC covers all three — remains the thing that must never happen.
        Assert.Equal(ErrorCode.InvalidRecord, response.Responses[0].PartitionResponses[0].ErrorCode);
        Assert.Equal(-1, response.Responses[0].PartitionResponses[0].BaseOffset);
    }

    [Fact]
    public async Task SectionTooShortForAHeader_IsRefusedAsInvalidRecord()
    {
        // Kafka: "must have at least one record batch per partition" — also InvalidRecord. Before,
        // this reached the header parsers and came back as a generic Unknown.
        var response = await ProduceAsync(new byte[20]);

        Assert.Equal(ErrorCode.InvalidRecord, response.Responses[0].PartitionResponses[0].ErrorCode);
    }

    [Fact]
    public async Task SectionWhoseFirstBatchOverrunsIt_IsRefusedAsInvalidRecord()
    {
        // A length field claiming more bytes than the section holds. The single-batch test is a
        // comparison, not a bounds check, so this must fail it rather than slip through as "one
        // batch" and be handed to the append.
        var batch = CreateValidBatch(baseOffset: 0, recordCount: 1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length * 4);

        var response = await ProduceAsync(batch);

        Assert.Equal(ErrorCode.InvalidRecord, response.Responses[0].PartitionResponses[0].ErrorCode);
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

using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// The Kafka fetch driven through the <b>real</b> File engine with memory-mapped reads, from the
/// request to the serialized response.
///
/// <para><b>Why this exists as its own suite.</b> The other lease tests lend a plain
/// <c>byte[]</c> from a fake segment. That reproduces "borrowed", but not the consequence: with the
/// real engine the record set is a projection over a raw pointer into a mapped view, so a lease
/// released one line too early is not scribbled bytes but an access violation that takes the
/// process down. Nothing built that object in a test before — the storage suite stopped at the
/// engine, the broker suite started at a fake (#78 audit).</para>
///
/// <para>The records deliberately sit at a file position that is not a multiple of the OS
/// allocation granularity, because that is where the mapped view's <c>PointerOffset</c> matters —
/// the bug that shipped hidden until mmap was activated.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class KafkaFetchOverMappedSegmentTests : IDisposable
{
    private const string Topic = "mapped-fetch-topic";

    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _transactionStateStore;
    private readonly QuotaManager _quotaManager;
    private readonly DataApiHandler _handler;

    public KafkaFetchOverMappedSegmentTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-mapped-fetch-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        // The real thing: file-backed segments with memory-mapped reads.
        _logManager = new LogManager(_dataDir, FileLogSegmentFactory.Create(useMmap: true), persistTopicsToFile: false);

        var config = new BrokerConfig();
        _offsetStore = new OffsetStore(_dataDir, NullLogger<OffsetStore>.Instance);
        _transactionStateStore = new TransactionStateStore(_dataDir, NullLogger<TransactionStateStore>.Instance);
        _quotaManager = new QuotaManager(config.Quotas, NullLogger<QuotaManager>.Instance);

        _handler = new DataApiHandler(
            config,
            _logManager,
            new TransactionCoordinator(
                new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore, _transactionStateStore,
                NullLogger<TransactionCoordinator>.Instance),
            _quotaManager,
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
        _quotaManager.Dispose();
        _logManager.Dispose();
        _offsetStore.Dispose();
        _transactionStateStore.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task Fetch_FromAMappedSegment_SerializesTheProducedBytes_ThenReleases()
    {
        var written = await AppendBatchesAsync(batchCount: 12, payloadBytes: 9_000);

        // Read from an offset in the middle: its file position is not allocation-granularity
        // aligned, so the mapped view is created at a rounded-down base and only PointerOffset
        // makes the slice land on the right bytes.
        const int fetchFromOffset = 7;

        var response = await FetchAsync(fetchFromOffset);
        var recordSet = response.Responses[0].Partitions[0].RecordSet;

        var expected = written.Skip(fetchFromOffset).SelectMany(b => b).ToArray();
        Assert.Equal(expected.Length, recordSet.Length);
        Assert.True(expected.AsSpan().SequenceEqual(recordSet.Span),
            "the record set does not match the produced bytes — a mapped view is being sliced at the wrong position");

        // Serializing is what the connection loop does before releasing, and it is the step that
        // reads every byte of the projection.
        using (var writer = new KafkaProtocolWriter())
        {
            response.WriteTo(writer);
            Assert.True(writer.WrittenSpan.Length > expected.Length,
                "the serialized response is smaller than its record set — nothing was written");
        }

        response.ReleaseBorrowedMemory();
    }

    [Fact]
    public async Task RealEngineRead_LendsItsBuffer_RatherThanQuietlyCopying()
    {
        // Everything above would also pass if the engine had quietly gone back to copying — the
        // bytes would be identical. This is the assertion that separates the two: a read that owns
        // its memory carries no lifetime, a borrowed one does. Without it the suite could keep
        // certifying a fetch path that lost its zero-copy property.
        await AppendBatchesAsync(batchCount: 4, payloadBytes: 8_192);

        using var read = await _logManager.ReadContiguousAsync(
            new TopicPartition { Topic = Topic, Partition = 0 }, startOffset: 0, maxBytes: 1024 * 1024);

        Assert.NotEqual(0, read.Data.Length);
        Assert.NotNull(read.Lifetime);
    }

    [Fact]
    public async Task RepeatedFetches_FromAMappedSegment_StayCorrect_AndReleaseTheirViews()
    {
        // Two hundred rounds against the real engine: a view released too early, released twice, or
        // never released shows up here as wrong bytes, a crash, or exhaustion — none of which the
        // fake-segment suite can produce.
        var written = await AppendBatchesAsync(batchCount: 6, payloadBytes: 4_096);
        var expected = written.SelectMany(b => b).ToArray();

        for (var round = 0; round < 200; round++)
        {
            var response = await FetchAsync(fetchOffset: 0);
            var recordSet = response.Responses[0].Partitions[0].RecordSet;

            Assert.True(expected.AsSpan().SequenceEqual(recordSet.Span), $"round {round} served different bytes");

            using (var writer = new KafkaProtocolWriter())
            {
                response.WriteTo(writer);
            }

            response.ReleaseBorrowedMemory();
        }
    }

    private async Task<List<byte[]>> AppendBatchesAsync(int batchCount, int payloadBytes)
    {
        await _logManager.CreateTopicAsync(Topic, partitionCount: 1);
        var tp = new TopicPartition { Topic = Topic, Partition = 0 };

        var written = new List<byte[]>(batchCount);
        for (var i = 0; i < batchCount; i++)
        {
            var batch = CreateRecordBatch(i, payloadBytes);
            await _logManager.AppendBatchAsync(tp, batch);
            written.Add(batch);
        }

        return written;
    }

    private async Task<FetchResponse> FetchAsync(int fetchOffset)
    {
        var response = await _handler.HandleAsync(
            new FetchRequest
            {
                ApiKey = ApiKey.Fetch,
                ApiVersion = 11,
                CorrelationId = 1,
                ClientId = "mapped-fetch",
                ReplicaId = -1,
                MaxWaitMs = 0,
                MinBytes = 1,
                MaxBytes = 8 * 1024 * 1024,
                Topics =
                [
                    new FetchRequest.FetchTopic
                    {
                        Topic = Topic,
                        Partitions =
                        [
                            new FetchRequest.FetchPartition
                            {
                                Partition = 0,
                                FetchOffset = fetchOffset,
                                MaxBytes = 8 * 1024 * 1024
                            }
                        ]
                    }
                ]
            },
            new RequestContext { ConnectionState = new ConnectionState("mapped-fetch"), ClientId = "mapped-fetch" },
            CancellationToken.None);

        var fetchResponse = Assert.IsType<FetchResponse>(response);
        Assert.Equal(ErrorCode.None, fetchResponse.Responses[0].Partitions[0].ErrorCode);
        return fetchResponse;
    }

    /// <summary>One record batch whose payload is recognisable per index.</summary>
    private static byte[] CreateRecordBatch(int index, int payloadBytes)
    {
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + payloadBytes];

        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), index);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);

        var marker = Encoding.UTF8.GetBytes($"batch-{index:D4}-");
        var body = batch.AsSpan(KafkaConstants.RecordBatch.HeaderSize);
        for (var i = 0; i < body.Length; i++)
            body[i] = marker[i % marker.Length];

        return batch;
    }
}

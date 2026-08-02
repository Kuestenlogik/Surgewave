using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Broker.Tests.Fakes;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// The Kafka fetch path serves record sets straight out of the storage lease instead of copying
/// them into a response-owned array (#78). That trades an allocation for a lifetime: the lease has
/// to survive until the response is written and has to be given back afterwards — exactly once,
/// for every partition of the fetch.
///
/// <para>The tests below pin both halves. They use a segment that hands its bytes out on loan and
/// scribbles over the buffer when the loan ends, so "borrowed, not copied" and "released, not
/// leaked" are observable rather than assumed — a copying implementation passes neither.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class KafkaFetchBorrowedReadTests : IDisposable
{
    private const string Topic = "fetch-lease-topic";

    private readonly string _dataDir;
    private readonly LeaseTrackingLogSegmentFactory _segmentFactory;
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _transactionStateStore;
    private readonly DataApiHandler _handler;

    public KafkaFetchBorrowedReadTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-fetch-lease-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _segmentFactory = new LeaseTrackingLogSegmentFactory(new MemoryLogSegmentFactory());
        _logManager = new LogManager(_dataDir, _segmentFactory, persistTopicsToFile: false);

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
    }

    public void Dispose()
    {
        _logManager.Dispose();
        _offsetStore.Dispose();
        _transactionStateStore.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task Fetch_ServesTheLeaseAndHoldsItUntilTheResponseIsReleased()
    {
        var written = await CreateTopicAndAppendAsync(partitionCount: 1, "borrowed-payload");

        var response = await FetchAsync(partitions: [0]);

        // Borrowed, not copied: the lease is still open while the response is unwritten, and the
        // bytes handed to the wire are the producer's.
        Assert.Equal(1, _segmentFactory.OpenLeases);
        var recordSet = response.Responses[0].Partitions[0].RecordSet;
        Assert.Equal(written[0], recordSet.ToArray());

        response.ReleaseBorrowedMemory();

        // Released, not leaked — and the payload really was the lease's own memory, which is why it
        // now carries the scribble instead of the record batch.
        Assert.Equal(0, _segmentFactory.OpenLeases);
        Assert.All(recordSet.ToArray(), b => Assert.Equal(LeaseTrackingLogSegment.ReleasedFill, b));
    }

    [Fact]
    public async Task Fetch_AcrossPartitions_ReleasesEveryLease()
    {
        // One lease per partition, so a response that only tracked a single one would leak the rest.
        var written = await CreateTopicAndAppendAsync(partitionCount: 3, "multi-partition-payload");

        var response = await FetchAsync(partitions: [0, 1, 2]);

        Assert.Equal(3, _segmentFactory.OpenLeases);
        for (var partition = 0; partition < 3; partition++)
            Assert.Equal(written[partition], response.Responses[0].Partitions[partition].RecordSet.ToArray());

        response.ReleaseBorrowedMemory();

        Assert.Equal(0, _segmentFactory.OpenLeases);
    }

    [Fact]
    public async Task Fetch_Repeated_KeepsServingCorrectBytes()
    {
        // A lease returned too early would surface here as scribbled-over bytes, one held too long
        // as a growing lease count. Both are the classic borrow-lifetime failures.
        var written = await CreateTopicAndAppendAsync(partitionCount: 1, "repeated-payload");

        for (var i = 0; i < 200; i++)
        {
            var response = await FetchAsync(partitions: [0]);
            Assert.Equal(written[0], response.Responses[0].Partitions[0].RecordSet.ToArray());
            response.ReleaseBorrowedMemory();
            Assert.Equal(0, _segmentFactory.OpenLeases);
        }
    }

    [Fact]
    public async Task ReleaseBorrowedMemory_IsIdempotent()
    {
        await CreateTopicAndAppendAsync(partitionCount: 1, "idempotent-release");

        var response = await FetchAsync(partitions: [0]);

        response.ReleaseBorrowedMemory();
        response.ReleaseBorrowedMemory();

        // A second release must not push the lease count below zero — i.e. must not hand the same
        // buffer back to the pool twice, which is how two partitions end up sharing memory.
        Assert.Equal(0, _segmentFactory.OpenLeases);
    }

    /// <summary>Creates the topic, appends one batch per partition and returns the written bytes.</summary>
    private async Task<byte[][]> CreateTopicAndAppendAsync(int partitionCount, string payload)
    {
        await _logManager.CreateTopicAsync(Topic, partitionCount);

        var written = new byte[partitionCount][];
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var batch = CreateRecordBatch($"{payload}-p{partition}");
            await _logManager.AppendBatchAsync(new TopicPartition { Topic = Topic, Partition = partition }, batch);
            written[partition] = batch;
        }

        return written;
    }

    private async Task<FetchResponse> FetchAsync(int[] partitions)
    {
        var response = await _handler.HandleAsync(
            new FetchRequest
            {
                ApiKey = ApiKey.Fetch,
                ApiVersion = 11,
                CorrelationId = 1,
                ClientId = "lease-test",
                ReplicaId = -1,
                MaxWaitMs = 0,
                MinBytes = 1,
                MaxBytes = 1024 * 1024,
                Topics =
                [
                    new FetchRequest.FetchTopic
                    {
                        Topic = Topic,
                        Partitions = [.. partitions.Select(p => new FetchRequest.FetchPartition
                        {
                            Partition = p,
                            FetchOffset = 0,
                            MaxBytes = 1024 * 1024
                        })]
                    }
                ]
            },
            new RequestContext { ConnectionState = new ConnectionState("lease-test"), ClientId = "lease-test" },
            CancellationToken.None);

        var fetchResponse = Assert.IsType<FetchResponse>(response);
        Assert.Equal(ErrorCode.None, fetchResponse.Responses[0].Partitions[0].ErrorCode);
        return fetchResponse;
    }

    private static byte[] CreateRecordBatch(string content)
    {
        var recordsData = Encoding.UTF8.GetBytes(content);
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + recordsData.Length];

        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);          // base offset
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12); // batch length
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);         // record count
        recordsData.CopyTo(batch, KafkaConstants.RecordBatch.HeaderSize);

        return batch;
    }
}

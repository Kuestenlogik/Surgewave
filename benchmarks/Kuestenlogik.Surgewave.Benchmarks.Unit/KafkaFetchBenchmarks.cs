using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// The Kafka fetch egress path end to end from the request to the served record set, on the File
/// engine — the configuration where the storage lease actually owns pooled or memory-mapped memory
/// (#78).
///
/// <para>Allocated bytes are the point here, not nanoseconds: the change under test removes the
/// payload-sized copy the response used to take per partition per fetch, so the expected signal is
/// allocation dropping by roughly the fetched size while the time barely moves. Both partition
/// counts are measured because the borrowed lifetime is per partition.</para>
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[BenchmarkCategory("Unit", "Protocol", "Fetch")]
public class KafkaFetchBenchmarks
{
    private const string Topic = "fetch-bench";
    private const int BatchPayloadBytes = 8 * 1024;
    private const int BatchesPerPartition = 8;

    private string _dataDir = null!;
    private LogManager _logManager = null!;
    private OffsetStore _offsetStore = null!;
    private TransactionStateStore _transactionStateStore = null!;
    private TransactionCoordinator _transactionCoordinator = null!;
    private QuotaManager _quotaManager = null!;
    private DataApiHandler _handler = null!;
    private RequestContext _context = null!;
    private FetchRequest _fetchOnePartition = null!;
    private FetchRequest _fetchEightPartitions = null!;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-fetch-bench-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _logManager = new LogManager(_dataDir, FileLogSegmentFactory.Create(), persistTopicsToFile: false);
        await _logManager.CreateTopicAsync(Topic, partitionCount: 8);

        var batch = CreateRecordBatch(BatchPayloadBytes);
        for (var partition = 0; partition < 8; partition++)
        {
            var tp = new TopicPartition { Topic = Topic, Partition = partition };
            for (var i = 0; i < BatchesPerPartition; i++)
                await _logManager.AppendBatchAsync(tp, batch);
        }

        var config = new BrokerConfig();
        _offsetStore = new OffsetStore(_dataDir, NullLogger<OffsetStore>.Instance);
        _transactionStateStore = new TransactionStateStore(_dataDir, NullLogger<TransactionStateStore>.Instance);
        _transactionCoordinator = new TransactionCoordinator(
            new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore, _transactionStateStore,
            NullLogger<TransactionCoordinator>.Instance);
        _quotaManager = new QuotaManager(config.Quotas, NullLogger<QuotaManager>.Instance);

        _handler = new DataApiHandler(
            config,
            _logManager,
            _transactionCoordinator,
            _quotaManager,
            new RecordBatchSerializer(NullLogger<RecordBatchSerializer>.Instance),
            aclAuthorizer: null,
            deduplicationManager: null,
            delayIndex: null,
            ttlIndex: null,
            metrics: null,
            NullLogger<DataApiHandler>.Instance);

        _context = new RequestContext { ConnectionState = new ConnectionState("fetch-bench"), ClientId = "fetch-bench" };
        _fetchOnePartition = CreateFetchRequest(partitionCount: 1);
        _fetchEightPartitions = CreateFetchRequest(partitionCount: 8);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _quotaManager.Dispose();
        _logManager.Dispose();
        _offsetStore.Dispose();
        _transactionStateStore.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Benchmark(Baseline = true)]
    public async Task<int> Fetch_SinglePartition()
    {
        var response = (FetchResponse)await _handler.HandleAsync(_fetchOnePartition, _context, CancellationToken.None);
        var bytes = response.Responses[0].Partitions[0].RecordSet.Length;
        response.ReleaseBorrowedMemory();
        return bytes;
    }

    [Benchmark]
    public async Task<int> Fetch_EightPartitions()
    {
        var response = (FetchResponse)await _handler.HandleAsync(_fetchEightPartitions, _context, CancellationToken.None);
        var bytes = 0;
        foreach (var partition in response.Responses[0].Partitions)
            bytes += partition.RecordSet.Length;
        response.ReleaseBorrowedMemory();
        return bytes;
    }

    private static FetchRequest CreateFetchRequest(int partitionCount)
    {
        var partitions = new List<FetchRequest.FetchPartition>(partitionCount);
        for (var partition = 0; partition < partitionCount; partition++)
        {
            partitions.Add(new FetchRequest.FetchPartition
            {
                Partition = partition,
                FetchOffset = 0,
                MaxBytes = 1024 * 1024
            });
        }

        return new FetchRequest
        {
            ApiKey = ApiKey.Fetch,
            ApiVersion = 11,
            CorrelationId = 1,
            ClientId = "fetch-bench",
            ReplicaId = -1,
            MaxWaitMs = 0,
            MinBytes = 1,
            MaxBytes = 8 * 1024 * 1024,
            Topics = [new FetchRequest.FetchTopic { Topic = Topic, Partitions = partitions }]
        };
    }

    private static byte[] CreateRecordBatch(int payloadBytes)
    {
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + payloadBytes];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        Random.Shared.NextBytes(batch.AsSpan(KafkaConstants.RecordBatch.HeaderSize));
        return batch;
    }
}

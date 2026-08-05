using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// The Kafka produce ingress path end to end from the request to the appended offset, on the File
/// engine.
///
/// <para>Until now the only gated produce number was <c>Parse_ProduceRequest</c> — the broker side
/// of the hot path was unmeasured, so any change to it landed unguarded. Durability work (#122) adds
/// decisions to exactly this method, and the contract for that work is that acks=0 and acks=1 pay
/// nothing: the acks comparison is hoisted once per request and the gate is never dereferenced
/// unless the client asked for acks=all. <c>Produce_AcksLeader</c> is the number that must not
/// move — in allocation at all, and in time beyond noise.</para>
///
/// <para><c>Produce_AcksAll</c> is measured against a gate that always admits, so it prices the
/// decision itself rather than any replication wait. Eight partitions are measured separately
/// because the decision sits inside the per-partition loop.</para>
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[BenchmarkCategory("Unit", "Protocol", "Produce")]
public class KafkaProduceBenchmarks
{
    private const string Topic = "produce-bench";
    private const int BatchPayloadBytes = 8 * 1024;

    private string _dataDir = null!;
    private LogManager _logManager = null!;
    private OffsetStore _offsetStore = null!;
    private TransactionStateStore _transactionStateStore = null!;
    private TransactionCoordinator _transactionCoordinator = null!;
    private QuotaManager _quotaManager = null!;
    private DataApiHandler _handler = null!;
    private RequestContext _context = null!;
    private ProduceRequest _acksLeaderOnePartition = null!;
    private ProduceRequest _acksAllOnePartition = null!;
    private ProduceRequest _acksLeaderEightPartitions = null!;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-produce-bench-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _logManager = new LogManager(_dataDir, FileLogSegmentFactory.Create(), persistTopicsToFile: false);
        await _logManager.CreateTopicAsync(Topic, partitionCount: 8);

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
        _handler.SetCommitGate(new AlwaysAdmitGate());

        _context = new RequestContext { ConnectionState = new ConnectionState("produce-bench"), ClientId = "produce-bench" };
        _acksLeaderOnePartition = CreateProduceRequest(acks: 1, partitionCount: 1);
        _acksAllOnePartition = CreateProduceRequest(acks: -1, partitionCount: 1);
        _acksLeaderEightPartitions = CreateProduceRequest(acks: 1, partitionCount: 8);
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
    public async Task<long> Produce_AcksLeader()
    {
        var response = (ProduceResponse)await _handler.HandleAsync(_acksLeaderOnePartition, _context, CancellationToken.None);
        return response.Responses[0].PartitionResponses[0].BaseOffset;
    }

    [Benchmark]
    public async Task<long> Produce_AcksAll()
    {
        var response = (ProduceResponse)await _handler.HandleAsync(_acksAllOnePartition, _context, CancellationToken.None);
        return response.Responses[0].PartitionResponses[0].BaseOffset;
    }

    [Benchmark]
    public async Task<long> Produce_AcksLeader_EightPartitions()
    {
        var response = (ProduceResponse)await _handler.HandleAsync(_acksLeaderEightPartitions, _context, CancellationToken.None);
        long last = 0;
        foreach (var partition in response.Responses[0].PartitionResponses)
            last = partition.BaseOffset;
        return last;
    }

    private static ProduceRequest CreateProduceRequest(short acks, int partitionCount)
    {
        var batch = CreateRecordBatch(BatchPayloadBytes);
        var partitions = new List<ProduceRequest.PartitionProduceData>(partitionCount);
        for (var partition = 0; partition < partitionCount; partition++)
        {
            partitions.Add(new ProduceRequest.PartitionProduceData
            {
                Index = partition,
                Records = batch
            });
        }

        return new ProduceRequest
        {
            ApiKey = ApiKey.Produce,
            ApiVersion = 9,
            CorrelationId = 1,
            ClientId = "produce-bench",
            RequiredAcks = acks,
            TimeoutMs = 30_000,
            TopicData = [new ProduceRequest.TopicProduceData { Name = Topic, PartitionData = partitions }]
        };
    }

    private static byte[] CreateRecordBatch(int payloadBytes)
    {
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + payloadBytes];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        batch[16] = 2;
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1); // no producer id: not idempotent
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        Random.Shared.NextBytes(batch.AsSpan(KafkaConstants.RecordBatch.HeaderSize));

        // The produce path appends with BatchCrcMode.Validate, so the payload has to carry a CRC
        // that matches its own bytes — a benchmark batch that fails validation measures the throw.
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);
        return batch;
    }

    /// <summary>
    /// Prices the durability decision without pricing replication: the wait is not part of this
    /// increment, and a gate that refused would measure the error path instead of the hot one.
    /// </summary>
    private sealed class AlwaysAdmitGate : IPartitionCommitGate
    {
        public bool CanAdmitDurableWrite(in TopicPartition partition) => true;
    }
}

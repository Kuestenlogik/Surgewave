using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Broker.Abstractions.Routing;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
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
/// The Kafka wire's side of what #268 found in the native protocol, asked for by #281: with topic auto-creation
/// on, a broker asked for a topic of the cluster that it hosts no replica of must not take the topic for a
/// missing one and go on to create it — the metadata log refuses that, the topic exists.
/// </summary>
/// <remarks>
/// It does not: produce and fetch never create a topic. These tests hold that, with a creator that records every
/// creation it is asked for and refuses it as the metadata log does. Metadata, the one Kafka request that
/// auto-creates, looks the topic up in the cluster's state first; NonControllerTopicCreationTests holds that.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class KafkaClusterTopicWithoutLocalReplicaTests : IDisposable
{
    private const string Topic = "led-elsewhere";

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-no-replica-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore = OffsetStore.InMemory();
    private readonly QuotaManager _quotaManager;
    private readonly RefusingCreator _creator = new();
    private readonly DataApiHandler _handler;

    public KafkaClusterTopicWithoutLocalReplicaTests()
    {
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _logManager.RouteTopicCreation(_creator);
        var config = new BrokerConfig { BrokerId = 2, AutoCreateTopics = true };
        _quotaManager = new QuotaManager(config.Quotas, NullLogger<QuotaManager>.Instance);
        var transactionCoordinator = new TransactionCoordinator(
            new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore,
            Kuestenlogik.Surgewave.Broker.Transactions.TransactionStateLog.InMemory(),
            NullLogger<TransactionCoordinator>.Instance);
        _handler = new DataApiHandler(
            config,
            _logManager,
            transactionCoordinator,
            _quotaManager,
            new RecordBatchSerializer(NullLogger<RecordBatchSerializer>.Instance),
            aclAuthorizer: null,
            deduplicationManager: null,
            delayIndex: null,
            ttlIndex: null,
            metrics: null,
            NullLogger<DataApiHandler>.Instance);
        _handler.SetPartitionLeadership(new LedByBrokerOne());
    }

    public void Dispose()
    {
        _quotaManager.Dispose();
        _logManager.Dispose();
        _offsetStore.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task ProduceIsAnsweredNotLeaderAndCreatesNothing()
    {
        var response = await _handler.HandleAsync(
            new ProduceRequest
            {
                ApiKey = ApiKey.Produce,
                ApiVersion = 9,
                CorrelationId = 1,
                ClientId = "no-replica-test",
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
            Context(),
            TestContext.Current.CancellationToken);

        var partition = Assert.IsType<ProduceResponse>(response).Responses[0].PartitionResponses[0];
        Assert.Equal(ErrorCode.NotLeaderForPartition, partition.ErrorCode);
        Assert.Empty(_creator.Asked);
        Assert.Null(_logManager.GetTopicMetadata(Topic));
    }

    /// <remarks>
    /// Answered, and nothing created, which is what is asked here. The answer itself is an empty fetch without an
    /// error: the fetch path does not ask who leads the partition, as the produce path does (#164). That is not
    /// what Kafka answers (NOT_LEADER_OR_FOLLOWER) and is left as found; it is not asserted either way.
    /// </remarks>
    [Fact]
    public async Task FetchIsAnsweredAndCreatesNothing()
    {
        var response = await _handler.HandleAsync(
            new FetchRequest
            {
                ApiKey = ApiKey.Fetch,
                ApiVersion = 11,
                CorrelationId = 2,
                ClientId = "no-replica-test",
                ReplicaId = -1,
                MaxWaitMs = 0,
                MinBytes = 1,
                MaxBytes = 1024 * 1024,
                Topics =
                [
                    new FetchRequest.FetchTopic
                    {
                        Topic = Topic,
                        Partitions = [new FetchRequest.FetchPartition { Partition = 0, FetchOffset = 0, MaxBytes = 1024 * 1024 }]
                    }
                ]
            },
            Context(),
            TestContext.Current.CancellationToken);

        Assert.Single(Assert.IsType<FetchResponse>(response).Responses[0].Partitions);
        Assert.Empty(_creator.Asked);
        Assert.Null(_logManager.GetTopicMetadata(Topic));
    }

    private static RequestContext Context() => new() { ConnectionState = new ConnectionState("no-replica-test"), ClientId = "no-replica-test" };

    private static ReadOnlyMemory<byte> Batch()
    {
        const int size = 100;
        var batch = new byte[size];
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), size - 12);
        batch[16] = 2;
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        return batch;
    }

    /// <summary>Every partition is led by broker 1, which this broker is not.</summary>
    private sealed class LedByBrokerOne : IPartitionLeadership
    {
        public bool IsLedByAnotherBroker(TopicPartition partition) => true;

        public PartitionLeader? LeaderOf(TopicPartition partition) => new PartitionLeader(1, 0);
    }

    /// <summary>The cluster's creator for a topic it already has: it refuses, as the metadata log does.</summary>
    private sealed class RefusingCreator : ITopicCreator
    {
        public List<string> Asked { get; } = [];

        public Task<TopicMetadata> CreateTopicAsync(
            string topicName, int partitionCount, short replicationFactor, Dictionary<string, string>? config,
            IReadOnlyList<IReadOnlyList<int>>? replicaAssignment, CancellationToken cancellationToken)
        {
            Asked.Add(topicName);
            return Task.FromException<TopicMetadata>(new InvalidOperationException($"Topic '{topicName}' already exists"));
        }
    }
}

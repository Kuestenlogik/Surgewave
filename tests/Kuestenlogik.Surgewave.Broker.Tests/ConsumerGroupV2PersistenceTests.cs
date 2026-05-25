using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Broker.GroupStatePersistence;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-848 9c: verifies that <see cref="ConsumerGroupV2Coordinator"/> reloads
/// its groups from a <see cref="JsonFileGroupStateStore{T}"/> after a simulated
/// broker restart. The store flushes synchronously on dispose, so a test that
/// disposes the first store before constructing the second is deterministic.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ConsumerGroupV2PersistenceTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly Guid _topicAId;

    private const string TopicA = "v2-persist-topic";

    public ConsumerGroupV2PersistenceTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-cg2-persist-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        var topic = _logManager.CreateTopicAsync(TopicA, partitionCount: 4).GetAwaiter().GetResult();
        _topicAId = topic.TopicId;
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void GroupState_SurvivesCoordinatorRestart()
    {
        // ── Phase 1: first coordinator instance creates a group and persists it.
        string memberId;
        int groupEpochBefore;
        using (var store = new JsonFileGroupStateStore<ConsumerGroupV2State>(_dataDir, "consumer-groups-v2", NullLogger.Instance))
        {
            var coordinator = new ConsumerGroupV2Coordinator(NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager, store);

            var resp = coordinator.HandleConsumerGroupHeartbeat(new ConsumerGroupHeartbeatRequest
            {
                ApiKey = ApiKey.ConsumerGroupHeartbeat,
                ApiVersion = 0,
                CorrelationId = 0,
                ClientId = "c1",
                GroupId = "persist-g",
                MemberId = "",
                MemberEpoch = 0,
                SubscribedTopicNames = [TopicA],
                RebalanceTimeoutMs = 60_000,
            });

            memberId = resp.MemberId!;
            groupEpochBefore = resp.MemberEpoch;
            // Disposing the store flushes the dirty bag synchronously.
        }

        // ── Phase 2: a fresh coordinator constructed with a NEW store instance pointing
        // at the same directory should rehydrate the group transparently.
        using var store2 = new JsonFileGroupStateStore<ConsumerGroupV2State>(_dataDir, "consumer-groups-v2", NullLogger.Instance);
        var coordinator2 = new ConsumerGroupV2Coordinator(NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager, store2);

        var describe = coordinator2.HandleConsumerGroupDescribe(new ConsumerGroupDescribeRequest
        {
            ApiKey = ApiKey.ConsumerGroupDescribe,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "test",
            GroupIds = ["persist-g"],
        });

        var group = Assert.Single(describe.Groups);
        Assert.Equal(ErrorCode.None, group.ErrorCode);
        var member = Assert.Single(group.Members);
        Assert.Equal(memberId, member.MemberId);
        Assert.Contains(TopicA, member.SubscribedTopicNames);
        Assert.True(group.GroupEpoch >= groupEpochBefore);
    }

    [Fact]
    public void GroupState_RemovedFromDisk_WhenLastMemberLeaves()
    {
        using var store = new JsonFileGroupStateStore<ConsumerGroupV2State>(_dataDir, "consumer-groups-v2", NullLogger.Instance);
        var coordinator = new ConsumerGroupV2Coordinator(NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager, store);

        var first = coordinator.HandleConsumerGroupHeartbeat(new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "c1",
            GroupId = "ephemeral-g",
            MemberId = "",
            MemberEpoch = 0,
            SubscribedTopicNames = [TopicA],
        });

        // Member leaves.
        coordinator.HandleConsumerGroupHeartbeat(new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "c1",
            GroupId = "ephemeral-g",
            MemberId = first.MemberId!,
            MemberEpoch = -1,
            SubscribedTopicNames = null,
        });

        var stateFiles = Directory.EnumerateFiles(Path.Combine(_dataDir, ".metadata", "consumer-groups-v2"), "*.json").ToList();
        Assert.DoesNotContain(stateFiles, p => Path.GetFileNameWithoutExtension(p).Contains("ephemeral", StringComparison.Ordinal));
    }

    [Fact]
    public void Coordinator_WithoutPersistence_StillWorks()
    {
        // The persistence parameter is optional; existing call sites must keep working.
        var coordinator = new ConsumerGroupV2Coordinator(
            NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager, persistence: null);

        var resp = coordinator.HandleConsumerGroupHeartbeat(new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "c1",
            GroupId = "no-persist-g",
            MemberId = "",
            MemberEpoch = 0,
            SubscribedTopicNames = [TopicA],
        });

        Assert.Equal(ErrorCode.None, resp.ErrorCode);
    }
}

using System.Reflection;
using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Broker.Hosting;
using Kuestenlogik.Surgewave.Broker.Queue;
using Kuestenlogik.Surgewave.Broker.ShareGroups;
using Kuestenlogik.Surgewave.Broker.StreamsGroups;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests that <see cref="GroupCoordinatorSweepService.SweepOnce"/> evicts stale
/// members from every coordinator type. We forcibly back-date the last-heartbeat
/// timestamps via reflection so the test runs in milliseconds rather than the
/// 30-45s the real timeout uses.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class GroupCoordinatorSweepServiceTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly ConsumerGroupV2Coordinator _v2;
    private readonly ShareGroupCoordinator _share;
    private readonly StreamsGroupCoordinator _streams;
    private readonly QueueViewManager _queueViewManager;
    private readonly GroupCoordinatorSweepService _sweep;

    public GroupCoordinatorSweepServiceTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-sweep-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _logManager.CreateTopicAsync("sweep-topic", partitionCount: 2).GetAwaiter().GetResult();

        var queueViewConfig = new QueueViewConfig
        {
            Enabled = true,
            VisibilityTimeout = TimeSpan.FromSeconds(30),
            MaxDeliveryCount = 3,
            CleanupInterval = TimeSpan.FromMinutes(60),
            MaxInFlightPerConsumer = 100,
        };
        _queueViewManager = new QueueViewManager(queueViewConfig, NullLoggerFactory.Instance, _logManager);

        _v2 = new ConsumerGroupV2Coordinator(NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager);
        _share = new ShareGroupCoordinator(NullLogger<ShareGroupCoordinator>.Instance, _logManager, _queueViewManager);
        _streams = new StreamsGroupCoordinator(NullLogger<StreamsGroupCoordinator>.Instance, _logManager);

        _sweep = new GroupCoordinatorSweepService(_v2, _share, _streams,
            NullLogger<GroupCoordinatorSweepService>.Instance, interval: TimeSpan.FromMilliseconds(50));
    }

    public void Dispose()
    {
        _sweep.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _queueViewManager.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void SweepOnce_RemovesStaleMembersFromConsumerGroupV2()
    {
        // Arrange: add a member, then back-date its heartbeat past the timeout.
        var resp = _v2.HandleConsumerGroupHeartbeat(new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "c1",
            GroupId = "sweep-g",
            MemberId = "",
            MemberEpoch = 0,
            SubscribedTopicNames = ["sweep-topic"],
        });
        Assert.Equal(ErrorCode.None, resp.ErrorCode);

        BackdateLastHeartbeat(_v2, "sweep-g", resp.MemberId!);

        // Act
        _sweep.SweepOnce();

        // Assert: describe should show no members.
        var described = _v2.HandleConsumerGroupDescribe(new ConsumerGroupDescribeRequest
        {
            ApiKey = ApiKey.ConsumerGroupDescribe,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "test",
            GroupIds = ["sweep-g"],
        });
        Assert.Empty(described.Groups[0].Members);
    }

    [Fact]
    public void SweepOnce_DoesNothing_WhenAllMembersFresh()
    {
        var resp = _v2.HandleConsumerGroupHeartbeat(new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "c1",
            GroupId = "sweep-g2",
            MemberId = "",
            MemberEpoch = 0,
            SubscribedTopicNames = ["sweep-topic"],
        });

        _sweep.SweepOnce();

        var described = _v2.HandleConsumerGroupDescribe(new ConsumerGroupDescribeRequest
        {
            ApiKey = ApiKey.ConsumerGroupDescribe,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "test",
            GroupIds = ["sweep-g2"],
        });
        Assert.Single(described.Groups[0].Members);
        Assert.Equal(resp.MemberId, described.Groups[0].Members[0].MemberId);
    }

    [Fact]
    public async Task PeriodicLoop_FiresSweepRepeatedly()
    {
        // Add a stale member, start the loop, observe the sweep removes it.
        var resp = _v2.HandleConsumerGroupHeartbeat(new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "c1",
            GroupId = "loop-g",
            MemberId = "",
            MemberEpoch = 0,
            SubscribedTopicNames = ["sweep-topic"],
        });
        BackdateLastHeartbeat(_v2, "loop-g", resp.MemberId!);

        await _sweep.StartAsync(CancellationToken.None);

        // Poll until the member is gone, max 2s.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        bool removed = false;
        while (DateTime.UtcNow < deadline)
        {
            var described = _v2.HandleConsumerGroupDescribe(new ConsumerGroupDescribeRequest
            {
                ApiKey = ApiKey.ConsumerGroupDescribe,
                ApiVersion = 0,
                CorrelationId = 0,
                ClientId = "test",
                GroupIds = ["loop-g"],
            });
            if (described.Groups[0].Members.Count == 0)
            {
                removed = true;
                break;
            }
            await Task.Delay(20);
        }
        Assert.True(removed, "Periodic sweep did not remove stale member within deadline.");
    }

    /// <summary>
    /// Reaches into the coordinator's private group state and back-dates a member's
    /// last-heartbeat timestamp past the stale timeout. Cheaper than waiting 45s.
    /// </summary>
    private static void BackdateLastHeartbeat(ConsumerGroupV2Coordinator coordinator, string groupId, string memberId)
    {
        var groupsField = typeof(ConsumerGroupV2Coordinator)
            .GetField("_groups", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var groups = groupsField.GetValue(coordinator)!;
        var indexer = groups.GetType().GetProperty("Item", new[] { typeof(string) })!;
        var group = indexer.GetValue(groups, [groupId])!;

        var membersProp = group.GetType().GetProperty("Members", BindingFlags.Public | BindingFlags.Instance)!;
        var members = membersProp.GetValue(group)!;
        var memberIndexer = members.GetType().GetProperty("Item", new[] { typeof(string) })!;
        var member = memberIndexer.GetValue(members, [memberId])!;

        var lastHeartbeatProp = member.GetType().GetProperty("LastHeartbeat")!;
        lastHeartbeatProp.SetValue(member, DateTime.UtcNow - TimeSpan.FromMinutes(5));
    }
}

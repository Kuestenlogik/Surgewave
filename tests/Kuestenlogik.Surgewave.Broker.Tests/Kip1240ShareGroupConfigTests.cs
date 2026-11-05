using Kuestenlogik.Surgewave.Broker.Queue;
using Kuestenlogik.Surgewave.Broker.ShareGroups;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-1240 — three share-group configs:
/// <list type="bullet">
///   <item><c>share.delivery.count.limit</c> (default 5, range 2-10)</item>
///   <item><c>share.partition.max.record.locks</c> (default 2000, range 100-10000)</item>
///   <item><c>share.renew.acknowledge.enable</c> (default true) — gates KIP-1222 RENEW ack-type.</item>
/// </list>
///
/// A group's configs are in the group config store — the metadata log in a cluster (#228) — and the coordinator runs
/// the group with them: a RENEW ack against a group where <c>share.renew.acknowledge.enable</c> is <c>false</c>
/// returns <see cref="ShareGroupErrorStatus.InvalidRequest"/> (per the KIP). Exercises the protocol-neutral coordinator
/// surface directly (#59).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1240ShareGroupConfigTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly QueueViewManager _queueViewManager;
    private readonly ShareGroupCoordinator _coordinator;
    private readonly InMemoryGroupConfigStore _groupConfigs = new();

    private const string Topic = "kip1240-topic";

    public Kip1240ShareGroupConfigTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-kip1240-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _logManager.CreateTopicAsync(Topic, partitionCount: 1).GetAwaiter().GetResult();

        _queueViewManager = new QueueViewManager(new QueueViewConfig
        {
            Enabled = true,
            VisibilityTimeout = TimeSpan.FromSeconds(30),
            MaxDeliveryCount = 3,
            CleanupInterval = TimeSpan.FromMinutes(60),
            MaxInFlightPerConsumer = 100,
        }, NullLoggerFactory.Instance, _logManager);

        _coordinator = new ShareGroupCoordinator(
            NullLogger<ShareGroupCoordinator>.Instance, _logManager, groupConfigs: _groupConfigs);
    }

    public void Dispose()
    {
        _queueViewManager.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void AGroupWithoutConfigs_RunsWithTheBrokersDefaults_WhichAreUpstreamKafkaKip1240Defaults()
    {
        var (deliveryLimit, maxRecordLocks, renewEnabled) = ShareGroupConfigs.Of(
            new Dictionary<string, string>(), ShareGroupDefaults.From(new BrokerConfig()));
        Assert.Equal(5, deliveryLimit);      // upstream: SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT
        Assert.Equal(2000, maxRecordLocks);  // upstream: SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DEFAULT
        Assert.True(renewEnabled);           // upstream: SHARE_RENEW_ACKNOWLEDGE_ENABLE_DEFAULT
    }

    [Fact]
    public async Task RenewAck_AgainstGroupWithRenewDisabled_ReturnsInvalidRequest()
    {
        // Set up: establish the group via a heartbeat, then turn renew off in the group's configs.
        var hb = await _coordinator.HeartbeatAsync(Join("c1", "g-renew-off"), TestContext.Current.CancellationToken);
        Assert.Null(await _coordinator.SetShareGroupConfigAsync("g-renew-off", "share.renew.acknowledge.enable", "false", TestContext.Current.CancellationToken));

        var ack = await _coordinator.ShareAcknowledgeAsync(RenewAck("g-renew-off"), TestContext.Current.CancellationToken);

        var partition = ack.Responses.Single().Partitions.Single();
        Assert.Equal(ShareGroupErrorStatus.InvalidRequest, partition.Status);
        Assert.NotEqual("", hb.MemberId);
    }

    [Fact]
    public async Task RenewAck_AgainstGroupWithRenewEnabled_DoesNotReturnInvalidRequest()
    {
        // Default group has RenewAcknowledgeEnabled=true (upstream default),
        // so a RENEW ack must NOT fail with InvalidRequest. The ack itself
        // may still error (UnknownTopicOrPartition because the QueueView
        // doesn't have the message ID), but the per-partition status
        // must not be InvalidRequest — that's what proves the gate is open.
        _ = await _coordinator.HeartbeatAsync(Join("c2", "g-renew-on"), TestContext.Current.CancellationToken);

        var ack = await _coordinator.ShareAcknowledgeAsync(RenewAck("g-renew-on"), TestContext.Current.CancellationToken);

        var partition = ack.Responses.Single().Partitions.Single();
        Assert.NotEqual(ShareGroupErrorStatus.InvalidRequest, partition.Status);
    }

    [Fact]
    public async Task RenewAck_AgainstUnknownGroup_UsesBrokerDefaultEnabled()
    {
        // Edge case: ack comes in before the group exists (clients can race
        // first-heartbeat with first-fetch in some configs). The coordinator
        // must NOT reject with InvalidRequest just because the group hasn't
        // been established — fallback is the broker default (true).
        var ack = await _coordinator.ShareAcknowledgeAsync(RenewAck("g-doesnt-exist-yet"), TestContext.Current.CancellationToken);

        var partition = ack.Responses.Single().Partitions.Single();
        Assert.NotEqual(ShareGroupErrorStatus.InvalidRequest, partition.Status);
    }

    private ShareGroupHeartbeatCommand Join(string clientId, string groupId)
        => new()
        {
            GroupId = groupId,
            MemberId = "",
            MemberEpoch = 0,
            ClientId = clientId,
            SubscribedTopicNames = [Topic],
        };

    private ShareAcknowledgeCommand RenewAck(string groupId)
        => new()
        {
            GroupId = groupId,
            Topics =
            [
                new ShareAcknowledgeTopic
                {
                    TopicId = _logManager.GetTopicId(Topic),
                    Partitions =
                    [
                        new ShareAcknowledgePartition
                        {
                            PartitionIndex = 0,
                            AcknowledgementBatches =
                            [
                                new ShareAcknowledgementBatch(0, 0, [(sbyte)4]), // Renew
                            ],
                        },
                    ],
                },
            ],
        };
}

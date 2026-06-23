using System.Reflection;
using System.Text.Json;
using Kuestenlogik.Surgewave.Broker.Queue;
using Kuestenlogik.Surgewave.Broker.ShareGroups;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
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
/// Surgewave captures all three as structural state on
/// <see cref="ShareGroupState"/>. <c>RenewAcknowledgeEnabled</c> is enforced
/// inline in the coordinator's ack-batch loop: a RENEW ack against a group
/// where the flag is <c>false</c> returns <see cref="ErrorCode.InvalidRequest"/>
/// (per the KIP). The other two are forward-compat state today — full
/// archive-on-overflow and per-partition lock-cap enforcement are
/// documented follow-ups (see kips.md).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1240ShareGroupConfigTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly QueueViewManager _queueViewManager;
    private readonly ShareGroupCoordinator _coordinator;

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
            NullLogger<ShareGroupCoordinator>.Instance, _logManager, _queueViewManager);
    }

    public void Dispose()
    {
        _queueViewManager.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void ShareGroupState_Defaults_MatchUpstreamKafkaKip1240Values()
    {
        var state = new ShareGroupState();
        Assert.Equal(5, state.MaxDeliveryCount);                  // upstream: SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT
        Assert.Equal(2000, state.MaxRecordLocks);                 // upstream: SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DEFAULT
        Assert.True(state.RenewAcknowledgeEnabled);               // upstream: SHARE_RENEW_ACKNOWLEDGE_ENABLE_DEFAULT
    }

    [Fact]
    public void ShareGroupState_JsonRoundTrip_PreservesAllThreeConfigs()
    {
        // KIP-932 9c uses System.Text.Json for share-group persistence; the
        // new fields must round-trip just like the rest of the state.
        var original = new ShareGroupState
        {
            GroupId = "g-roundtrip",
            GroupEpoch = 3,
            MaxDeliveryCount = 7,
            MaxRecordLocks = 1500,
            RenewAcknowledgeEnabled = false,
        };

        var json = JsonSerializer.Serialize(original);
        var roundtripped = JsonSerializer.Deserialize<ShareGroupState>(json)!;

        Assert.Equal(7, roundtripped.MaxDeliveryCount);
        Assert.Equal(1500, roundtripped.MaxRecordLocks);
        Assert.False(roundtripped.RenewAcknowledgeEnabled);
        // Sanity: existing fields still survive
        Assert.Equal("g-roundtrip", roundtripped.GroupId);
        Assert.Equal(3, roundtripped.GroupEpoch);
    }

    [Fact]
    public void RenewAck_AgainstGroupWithRenewDisabled_ReturnsInvalidRequest()
    {
        // Set up: establish the group via a heartbeat, then flip
        // RenewAcknowledgeEnabled to false (the IncrementalAlterConfigs path
        // for share groups isn't wired yet — see kips.md follow-up).
        var hb = _coordinator.HandleShareGroupHeartbeat(new ShareGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ShareGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "c1",
            GroupId = "g-renew-off",
            MemberId = "",
            MemberEpoch = 0,
            SubscribedTopicNames = [Topic],
        });
        Assert.Equal(ErrorCode.None, hb.ErrorCode);

        FlipRenewEnabled("g-renew-off", false);

        var ack = _coordinator.HandleShareAcknowledge(new ShareAcknowledgeRequest
        {
            ApiKey = ApiKey.ShareAcknowledge,
            ApiVersion = 2,
            CorrelationId = 1,
            ClientId = "c1",
            GroupId = "g-renew-off",
            MemberId = hb.MemberId,
            ShareSessionEpoch = 1,
            IsRenewAck = true,
            Topics =
            [
                new ShareAcknowledgeRequest.AcknowledgeTopic
                {
                    TopicId = _logManager.GetTopicId(Topic),
                    Partitions =
                    [
                        new ShareAcknowledgeRequest.AcknowledgePartition
                        {
                            PartitionIndex = 0,
                            AcknowledgementBatches =
                            [
                                new ShareAcknowledgeRequest.AcknowledgementBatch
                                {
                                    FirstOffset = 0,
                                    LastOffset = 0,
                                    AcknowledgeTypes = [(sbyte)4], // Renew
                                },
                            ],
                        },
                    ],
                },
            ],
        });

        var partition = ack.Responses.Single().Partitions.Single();
        Assert.Equal(ErrorCode.InvalidRequest, partition.ErrorCode);
    }

    [Fact]
    public void RenewAck_AgainstGroupWithRenewEnabled_DoesNotReturnInvalidRequest()
    {
        // Default group has RenewAcknowledgeEnabled=true (upstream default),
        // so a RENEW ack must NOT fail with InvalidRequest. The ack itself
        // may still error (UnknownTopicOrPartition because the QueueView
        // doesn't have the message ID), but the per-partition error code
        // must not be InvalidRequest — that's what proves the gate is open.
        var hb = _coordinator.HandleShareGroupHeartbeat(new ShareGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ShareGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = "c2",
            GroupId = "g-renew-on",
            MemberId = "",
            MemberEpoch = 0,
            SubscribedTopicNames = [Topic],
        });
        Assert.Equal(ErrorCode.None, hb.ErrorCode);

        var ack = _coordinator.HandleShareAcknowledge(new ShareAcknowledgeRequest
        {
            ApiKey = ApiKey.ShareAcknowledge,
            ApiVersion = 2,
            CorrelationId = 1,
            ClientId = "c2",
            GroupId = "g-renew-on",
            MemberId = hb.MemberId,
            ShareSessionEpoch = 1,
            IsRenewAck = true,
            Topics =
            [
                new ShareAcknowledgeRequest.AcknowledgeTopic
                {
                    TopicId = _logManager.GetTopicId(Topic),
                    Partitions =
                    [
                        new ShareAcknowledgeRequest.AcknowledgePartition
                        {
                            PartitionIndex = 0,
                            AcknowledgementBatches =
                            [
                                new ShareAcknowledgeRequest.AcknowledgementBatch
                                {
                                    FirstOffset = 0,
                                    LastOffset = 0,
                                    AcknowledgeTypes = [(sbyte)4], // Renew
                                },
                            ],
                        },
                    ],
                },
            ],
        });

        var partition = ack.Responses.Single().Partitions.Single();
        Assert.NotEqual(ErrorCode.InvalidRequest, partition.ErrorCode);
    }

    [Fact]
    public void RenewAck_AgainstUnknownGroup_UsesBrokerDefaultEnabled()
    {
        // Edge case: ack comes in before the group exists (clients can race
        // first-heartbeat with first-fetch in some configs). The coordinator
        // must NOT reject with InvalidRequest just because the group hasn't
        // been established — fallback is the broker default (true).
        var ack = _coordinator.HandleShareAcknowledge(new ShareAcknowledgeRequest
        {
            ApiKey = ApiKey.ShareAcknowledge,
            ApiVersion = 2,
            CorrelationId = 1,
            ClientId = "c3",
            GroupId = "g-doesnt-exist-yet",
            MemberId = "anyone",
            ShareSessionEpoch = 0,
            IsRenewAck = true,
            Topics =
            [
                new ShareAcknowledgeRequest.AcknowledgeTopic
                {
                    TopicId = _logManager.GetTopicId(Topic),
                    Partitions =
                    [
                        new ShareAcknowledgeRequest.AcknowledgePartition
                        {
                            PartitionIndex = 0,
                            AcknowledgementBatches =
                            [
                                new ShareAcknowledgeRequest.AcknowledgementBatch
                                {
                                    FirstOffset = 0,
                                    LastOffset = 0,
                                    AcknowledgeTypes = [(sbyte)4],
                                },
                            ],
                        },
                    ],
                },
            ],
        });

        var partition = ack.Responses.Single().Partitions.Single();
        Assert.NotEqual(ErrorCode.InvalidRequest, partition.ErrorCode);
    }

    /// <summary>
    /// Reaches into the coordinator's private <c>_shareGroups</c> dictionary to
    /// flip the per-group flag. The IncrementalAlterConfigs admin-side wire-up
    /// for share-group configs is a documented follow-up; until that lands the
    /// flag is only mutable internally, so the test goes via reflection.
    /// </summary>
    private void FlipRenewEnabled(string groupId, bool value)
    {
        var field = typeof(ShareGroupCoordinator).GetField("_shareGroups", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var dict = (Dictionary<string, ShareGroupState>)field.GetValue(_coordinator)!;
        var lockField = typeof(ShareGroupCoordinator).GetField("_groupLock", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var groupLock = lockField.GetValue(_coordinator)!;
        lock (groupLock)
        {
            dict[groupId].RenewAcknowledgeEnabled = value;
        }
    }
}

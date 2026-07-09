using System.Reflection;
using Kuestenlogik.Surgewave.Broker.Handlers;
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
/// KIP-1240 follow-up — IncrementalAlterConfigs admin wire-up for share-group
/// configs. The earlier KIP-1240 commit captured the three configs as state on
/// <see cref="ShareGroupState"/> and enforced <c>share.renew.acknowledge.enable</c>
/// inline in the coordinator's ack-batch loop, but admins could only mutate
/// the flag via reflection in tests. This commit wires the admin path:
/// <c>ConfigResourceType.Group (32)</c> + <c>IncrementalAlterConfigsRequest</c>
/// → <see cref="ShareGroupCoordinator.SetShareGroupConfig"/>.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1240IncrementalAlterConfigsTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly QueueViewManager _queueViewManager;
    private readonly ShareGroupCoordinator _coordinator;
    private readonly ConfigApiHandler _configHandler;

    public Kip1240IncrementalAlterConfigsTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-kip1240-admin-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
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

        var brokerConfig = new BrokerConfig { DataDirectory = _dataDir };
        var dynamicConfig = new DynamicBrokerConfig(brokerConfig, NullLogger<DynamicBrokerConfig>.Instance);
        _configHandler = new ConfigApiHandler(brokerConfig, dynamicConfig, _logManager, _coordinator);
    }

    public void Dispose()
    {
        _queueViewManager.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    private static IncrementalAlterConfigsRequest BuildRequest(string groupId, string configName, sbyte op, string? value) =>
        new()
        {
            ApiKey = ApiKey.IncrementalAlterConfigs,
            ApiVersion = 1,
            CorrelationId = 1,
            ClientId = "kip1240-admin",
            ValidateOnly = false,
            Resources =
            [
                new IncrementalAlterConfigsRequest.AlterConfigsResource
                {
                    ResourceType = (sbyte)ConfigResourceType.Group, // 32
                    ResourceName = groupId,
                    Configs =
                    [
                        new IncrementalAlterConfigsRequest.AlterableConfig
                        {
                            Name = configName,
                            ConfigOperation = op,
                            Value = value,
                        },
                    ],
                },
            ],
        };

    private async Task<IncrementalAlterConfigsResponse> InvokeAsync(IncrementalAlterConfigsRequest request)
    {
        var ctx = new RequestContext
        {
            ConnectionState = new Kuestenlogik.Surgewave.Protocol.Kafka.ConnectionState("127.0.0.1"),
            ClientId = request.ClientId,
        };
        var response = await _configHandler.HandleAsync(request, ctx, CancellationToken.None);
        return (IncrementalAlterConfigsResponse)response;
    }

    /// <summary>
    /// Reach into the coordinator's private state map to verify the
    /// IncrementalAlterConfigs round-trip actually persisted. Production
    /// code uses LookupRenewEnabled which we can't see from here.
    /// </summary>
    private T ReadGroupField<T>(string groupId, string fieldName)
    {
        var groupsField = typeof(ShareGroupCoordinator).GetField("_shareGroups", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var lockField = typeof(ShareGroupCoordinator).GetField("_groupLock", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var lockObj = lockField.GetValue(_coordinator)!;
        var groups = (Dictionary<string, ShareGroupState>)groupsField.GetValue(_coordinator)!;
        lock (lockObj)
        {
            var prop = typeof(ShareGroupState).GetProperty(fieldName)!;
            return (T)prop.GetValue(groups[groupId])!;
        }
    }

    [Fact]
    public async Task SetRenewAcknowledgeEnableFalse_PersistsOnShareGroupState()
    {
        var response = await InvokeAsync(BuildRequest("g1", "share.renew.acknowledge.enable", op: 0, value: "false"));
        Assert.Equal(ErrorCode.None, response.Responses[0].ErrorCode);
        Assert.False(ReadGroupField<bool>("g1", nameof(ShareGroupState.RenewAcknowledgeEnabled)));
    }

    [Fact]
    public async Task SetDeliveryCountLimit_RespectsKip1240Clamp()
    {
        // Upper bound is 10 per the KIP; 11 must fail.
        var tooHigh = await InvokeAsync(BuildRequest("g2", "share.delivery.count.limit", op: 0, value: "11"));
        Assert.Equal(ErrorCode.InvalidConfig, tooHigh.Responses[0].ErrorCode);
        Assert.Contains("between 2 and 10", tooHigh.Responses[0].ErrorMessage, StringComparison.Ordinal);

        // 2 is the lower bound — must succeed.
        var atLowerBound = await InvokeAsync(BuildRequest("g2", "share.delivery.count.limit", op: 0, value: "2"));
        Assert.Equal(ErrorCode.None, atLowerBound.Responses[0].ErrorCode);
    }

    [Fact]
    public async Task SetMaxRecordLocks_RespectsKip1240Clamp()
    {
        // Lower bound 100, upper 10000.
        var tooLow = await InvokeAsync(BuildRequest("g3", "share.partition.max.record.locks", op: 0, value: "99"));
        Assert.Equal(ErrorCode.InvalidConfig, tooLow.Responses[0].ErrorCode);

        var atUpperBound = await InvokeAsync(BuildRequest("g3", "share.partition.max.record.locks", op: 0, value: "10000"));
        Assert.Equal(ErrorCode.None, atUpperBound.Responses[0].ErrorCode);
    }

    [Fact]
    public async Task DeleteOperation_RestoresUpstreamDefault()
    {
        // First set to non-default, then DELETE — should restore to upstream default.
        await InvokeAsync(BuildRequest("g4", "share.delivery.count.limit", op: 0, value: "10"));
        var deleteResp = await InvokeAsync(BuildRequest("g4", "share.delivery.count.limit", op: 1, value: null));
        Assert.Equal(ErrorCode.None, deleteResp.Responses[0].ErrorCode);
        Assert.Equal(5, ReadGroupField<int>("g4", nameof(ShareGroupState.MaxDeliveryCount))); // upstream default
    }

    [Fact]
    public async Task UnknownConfigName_ReturnsInvalidConfig()
    {
        var response = await InvokeAsync(BuildRequest("g5", "share.bogus.config", op: 0, value: "anything"));
        Assert.Equal(ErrorCode.InvalidConfig, response.Responses[0].ErrorCode);
        Assert.Contains("not a recognized share-group config", response.Responses[0].ErrorMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ValidateOnly_DoesNotMutateState()
    {
        // First set a non-default value.
        await InvokeAsync(BuildRequest("g6", "share.delivery.count.limit", op: 0, value: "8"));
        Assert.Equal(8, ReadGroupField<int>("g6", nameof(ShareGroupState.MaxDeliveryCount)));

        // Now send a ValidateOnly request with a different value — state must stay 8.
        var validateRequest = new IncrementalAlterConfigsRequest
        {
            ApiKey = ApiKey.IncrementalAlterConfigs,
            ApiVersion = 1,
            CorrelationId = 1,
            ClientId = "kip1240-admin",
            ValidateOnly = true,
            Resources =
            [
                new IncrementalAlterConfigsRequest.AlterConfigsResource
                {
                    ResourceType = (sbyte)ConfigResourceType.Group,
                    ResourceName = "g6",
                    Configs =
                    [
                        new IncrementalAlterConfigsRequest.AlterableConfig
                        {
                            Name = "share.delivery.count.limit",
                            ConfigOperation = 0,
                            Value = "3",
                        },
                    ],
                },
            ],
        };
        var response = await InvokeAsync(validateRequest);
        Assert.Equal(ErrorCode.None, response.Responses[0].ErrorCode);
        Assert.Equal(8, ReadGroupField<int>("g6", nameof(ShareGroupState.MaxDeliveryCount))); // unchanged
    }
}

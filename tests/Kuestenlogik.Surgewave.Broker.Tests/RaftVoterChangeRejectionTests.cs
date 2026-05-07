using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-853 wire surface — Surgewave advertises AddRaftVoter (80), RemoveRaftVoter
/// (81), and UpdateRaftVoter (82) so admin tools can probe the API but the
/// underlying online-reconfiguration state machine is not implemented yet.
/// The handler must return <see cref="ErrorCode.UnsupportedVersion"/> with
/// a stable, machine-readable message documenting the limitation. Without
/// these tests a future "let's just remove the stub handler" refactor would
/// silently fall back to the dispatcher's generic UNSUPPORTED_VERSION
/// (functionally identical, but admin tools would lose the precise reason).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftVoterChangeRejectionTests
{
    [Fact]
    public async Task AddRaftVoter_ReturnsUnsupportedWithDocumentedReason()
    {
        var handler = BuildHandler();
        var response = await handler.HandleAsync(
            new AddRaftVoterRequest
            {
                ApiKey = ApiKey.AddRaftVoter,
                ApiVersion = 0,
                CorrelationId = 7,
                ClientId = "admin",
                ClusterId = "cluster",
                TimeoutMs = 5000,
                VoterId = 99,
                VoterDirectoryId = Guid.NewGuid(),
                Listeners = [new AddRaftVoterRequest.ListenerInfo { Name = "INTERNAL", Host = "h", Port = 9092 }],
            },
            new RequestContext { ConnectionState = new ConnectionState("test-host"), ClientId = "admin" },
            CancellationToken.None) as AddRaftVoterResponse;

        Assert.NotNull(response);
        Assert.Equal(7, response!.CorrelationId);
        Assert.Equal(ErrorCode.UnsupportedVersion, response.ErrorCode);
        Assert.NotNull(response.ErrorMessage);
        Assert.Contains("KIP-853", response.ErrorMessage);
    }

    [Fact]
    public async Task RemoveRaftVoter_ReturnsUnsupportedWithDocumentedReason()
    {
        var handler = BuildHandler();
        var response = await handler.HandleAsync(
            new RemoveRaftVoterRequest
            {
                ApiKey = ApiKey.RemoveRaftVoter,
                ApiVersion = 0,
                CorrelationId = 11,
                ClientId = "admin",
                ClusterId = "cluster",
                VoterId = 42,
                VoterDirectoryId = Guid.NewGuid(),
            },
            new RequestContext { ConnectionState = new ConnectionState("test-host"), ClientId = "admin" },
            CancellationToken.None) as RemoveRaftVoterResponse;

        Assert.NotNull(response);
        Assert.Equal(11, response!.CorrelationId);
        Assert.Equal(ErrorCode.UnsupportedVersion, response.ErrorCode);
        Assert.Contains("KIP-853", response.ErrorMessage);
    }

    [Fact]
    public async Task UpdateRaftVoter_ReturnsUnsupportedAndDoesNotThrow()
    {
        var handler = BuildHandler();
        var response = await handler.HandleAsync(
            new UpdateRaftVoterRequest
            {
                ApiKey = ApiKey.UpdateRaftVoter,
                ApiVersion = 0,
                CorrelationId = 13,
                ClientId = "admin",
                ClusterId = "cluster",
                CurrentLeaderEpoch = 1,
                VoterId = 7,
                VoterDirectoryId = Guid.NewGuid(),
                Listeners = [new UpdateRaftVoterRequest.ListenerInfo { Name = "INTERNAL", Host = "h", Port = 9092 }],
                KRaftVersionFeature = new UpdateRaftVoterRequest.KRaftVersionFeatureInfo
                {
                    MinSupportedVersion = 0,
                    MaxSupportedVersion = 1,
                },
            },
            new RequestContext { ConnectionState = new ConnectionState("test-host"), ClientId = "admin" },
            CancellationToken.None) as UpdateRaftVoterResponse;

        Assert.NotNull(response);
        Assert.Equal(13, response!.CorrelationId);
        Assert.Equal(ErrorCode.UnsupportedVersion, response.ErrorCode);
    }

    [Fact]
    public void HandlerAdvertisesAllThreeKip853ApiKeys()
    {
        var handler = BuildHandler();
        var keys = handler.SupportedApiKeys.ToHashSet();
        Assert.Contains(ApiKey.AddRaftVoter, keys);
        Assert.Contains(ApiKey.RemoveRaftVoter, keys);
        Assert.Contains(ApiKey.UpdateRaftVoter, keys);
    }

    [Fact]
    public async Task AddRaftVoter_NegativeVoterId_RejectedWithInvalidRequest()
    {
        // Shape pre-validation runs before the not-supported reply. A
        // malformed request gets the precise protocol error instead of a
        // misleading "feature off" message.
        var handler = BuildHandler();
        var resp = await handler.HandleAsync(
            new AddRaftVoterRequest
            {
                ApiKey = ApiKey.AddRaftVoter,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                ClusterId = "cluster",
                TimeoutMs = 1000,
                VoterId = -1,
                VoterDirectoryId = Guid.NewGuid(),
                Listeners = [new AddRaftVoterRequest.ListenerInfo { Name = "INTERNAL", Host = "h", Port = 9092 }],
            },
            new RequestContext { ConnectionState = new ConnectionState("h"), ClientId = "admin" },
            CancellationToken.None) as AddRaftVoterResponse;

        Assert.Equal(ErrorCode.InvalidRequest, resp!.ErrorCode);
        Assert.Contains("non-negative", resp.ErrorMessage);
    }

    [Fact]
    public async Task AddRaftVoter_NoListeners_RejectedWithInvalidRequest()
    {
        var handler = BuildHandler();
        var resp = await handler.HandleAsync(
            new AddRaftVoterRequest
            {
                ApiKey = ApiKey.AddRaftVoter,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                ClusterId = "cluster",
                TimeoutMs = 1000,
                VoterId = 1,
                VoterDirectoryId = Guid.NewGuid(),
                Listeners = [],
            },
            new RequestContext { ConnectionState = new ConnectionState("h"), ClientId = "admin" },
            CancellationToken.None) as AddRaftVoterResponse;

        Assert.Equal(ErrorCode.InvalidRequest, resp!.ErrorCode);
        Assert.Contains("listener", resp.ErrorMessage);
    }

    private static RaftApiHandler BuildHandler()
    {
        // raftNode and raftPersistence are deliberately null — the KIP-853
        // path runs before the null-check so the handler can answer voter
        // queries even when consensus is disabled.
        var config = new BrokerConfig { BrokerId = 1 };
        var clusterState = new ClusterState();
        return new RaftApiHandler(
            config,
            raftNode: null,
            raftPersistence: null,
            clusterState,
            NullLogger<RaftApiHandler>.Instance);
    }
}

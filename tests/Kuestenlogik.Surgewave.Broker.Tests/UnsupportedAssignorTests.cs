using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Broker.Native.Assignors;
using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// A client that asks for an assignor this broker does not have gets told so (#127).
///
/// <para>The name arrives on the wire — KIP-848's <c>ServerAssignor</c> in the heartbeat — and the
/// broker used to substitute its default for anything it did not recognise. That is the wrong
/// answer to a request for something specific: the client keeps its own expectations about which
/// partitions move on a rebalance, so a typo in <c>group.remote.assignor</c> surfaces months later
/// as unexplained reassignment instead of as an error somebody can act on.</para>
///
/// <para>Kafka reserves <c>UNSUPPORTED_ASSIGNOR</c> (112) for exactly this, and the code already
/// existed here unused.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class UnsupportedAssignorTests : IDisposable
{
    private const string Topic = "assignor-topic";

    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly ConsumerGroupV2Coordinator _coordinator;

    public UnsupportedAssignorTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-assignor-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _logManager.CreateTopicAsync(Topic, partitionCount: 4).GetAwaiter().GetResult();
        _coordinator = new ConsumerGroupV2Coordinator(
            NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager, persistence: null);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void AnUnknownAssignor_IsRefused()
    {
        var result = _coordinator.Heartbeat(Heartbeat("g-unknown", serverAssignor: "stikcy"));

        Assert.Equal(ConsumerGroupFenceStatus.UnsupportedAssignor, result.Status);
    }

    [Fact]
    public void AnUnknownAssignor_LeavesTheGroupUntouched()
    {
        // The refusal happens before any mutation, like the epoch fence. A request the broker will
        // not honour must not advance the group — otherwise a client with a typo repeatedly bumps
        // the group epoch and forces rebalances it never gets to take part in.
        _coordinator.Heartbeat(Heartbeat("g-untouched", serverAssignor: "sticky"));
        var before = Assert.Single(_coordinator.Describe(["g-untouched"]));

        _coordinator.Heartbeat(Heartbeat("g-untouched", serverAssignor: "nonsense"));
        var after = Assert.Single(_coordinator.Describe(["g-untouched"]));

        Assert.Equal(before.GroupEpoch, after.GroupEpoch);
        Assert.Equal(before.AssignorName, after.AssignorName);
    }

    [Theory]
    [InlineData("range")]
    [InlineData("roundrobin")]
    [InlineData("sticky")]
    [InlineData("cooperative-sticky")]
    [InlineData("STICKY")]
    public void AKnownAssignor_IsAccepted(string assignorName)
    {
        var result = _coordinator.Heartbeat(Heartbeat($"g-{assignorName}", assignorName));

        Assert.NotEqual(ConsumerGroupFenceStatus.UnsupportedAssignor, result.Status);
    }

    [Fact]
    public void NoAssignorRequested_IsNotARefusal()
    {
        // Expressing no preference is legitimate and keeps the broker's default.
        var result = _coordinator.Heartbeat(Heartbeat("g-none", serverAssignor: null));

        Assert.NotEqual(ConsumerGroupFenceStatus.UnsupportedAssignor, result.Status);
    }

    [Fact]
    public void TryGetAssignor_DoesNotSubstitute()
    {
        Assert.False(PartitionAssignorFactory.TryGetAssignor("nonsense", out _));
        Assert.True(PartitionAssignorFactory.TryGetAssignor("sticky", out var sticky));
        Assert.Equal("sticky", sticky.Name, ignoreCase: true);
    }

    private static ConsumerHeartbeatCommand Heartbeat(string groupId, string? serverAssignor) => new()
    {
        ClientId = "assignor-test",
        GroupId = groupId,
        MemberId = "",
        MemberEpoch = 0,
        SubscribedTopicNames = [Topic],
        RebalanceTimeoutMs = 60_000,
        ServerAssignor = serverAssignor,
    };
}

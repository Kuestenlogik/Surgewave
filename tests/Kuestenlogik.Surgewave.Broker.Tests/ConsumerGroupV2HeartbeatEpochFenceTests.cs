using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-848 heartbeat-epoch fencing (G9 hardening). Without these checks a
/// stale or replayed heartbeat would silently inherit the current group epoch
/// and walk back into reconciliation with state the broker never authorised.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ConsumerGroupV2HeartbeatEpochFenceTests : IDisposable
{
    private const string Topic = "fence-topic";
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly ConsumerGroupV2Coordinator _coordinator;

    public ConsumerGroupV2HeartbeatEpochFenceTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-cg2-fence-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _logManager.CreateTopicAsync(Topic, partitionCount: 4).GetAwaiter().GetResult();
        _coordinator = new ConsumerGroupV2Coordinator(
            NullLogger<ConsumerGroupV2Coordinator>.Instance,
            _logManager);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void Heartbeat_StaleEpoch_ReturnsStaleMemberEpoch()
    {
        // Member joins (epoch=0 → broker assigns epoch=1).
        var join = Heartbeat("g", "c1", memberId: "", memberEpoch: 0);
        Assert.Equal(ErrorCode.None, join.ErrorCode);
        Assert.True(join.MemberEpoch >= 1);
        var assignedId = join.MemberId!;
        var currentEpoch = join.MemberEpoch;

        // Replay a heartbeat from before the join — same memberId but with
        // an older epoch that the client never reset.
        var stale = Heartbeat("g", "c1", memberId: assignedId, memberEpoch: currentEpoch - 1);

        Assert.Equal(ErrorCode.StaleMemberEpoch, stale.ErrorCode);
        // The fenced response must NOT carry an assignment — the client has
        // to resync via a fresh init before it gets state again.
        Assert.Null(stale.MemberAssignment);
    }

    [Fact]
    public void Heartbeat_FutureEpoch_ReturnsFencedMemberEpoch()
    {
        var join = Heartbeat("g", "c1", memberId: "", memberEpoch: 0);
        Assert.Equal(ErrorCode.None, join.ErrorCode);
        var assignedId = join.MemberId!;

        // Future-epoch heartbeat — impossible state, the broker never issued
        // this. KIP-848 fences it as FENCED_MEMBER_EPOCH so the client
        // discards local state and rejoins.
        var fenced = Heartbeat("g", "c1", memberId: assignedId, memberEpoch: join.MemberEpoch + 5);

        Assert.Equal(ErrorCode.FencedMemberEpoch, fenced.ErrorCode);
    }

    [Fact]
    public void Heartbeat_NonZeroEpoch_UnknownMember_ReturnsUnknownMemberId()
    {
        // No previous join — but a client claims a memberId AND a non-zero
        // epoch. This is structurally impossible (the broker assigns the id
        // on the epoch-0 join) and must be UNKNOWN_MEMBER_ID.
        var resp = Heartbeat("g", "c1", memberId: "ghost-member", memberEpoch: 7);

        Assert.Equal(ErrorCode.UnknownMemberId, resp.ErrorCode);
    }

    [Fact]
    public void Heartbeat_ZeroEpoch_UnknownMember_StillJoinsCleanly()
    {
        // Epoch=0 with a non-empty memberId is the static-membership rejoin
        // path — the client claims a stable id. Surgewave accepts this so static
        // members can carry their identity across restarts; the fence MUST
        // NOT trip.
        var resp = Heartbeat("g", "c1", memberId: "static-1", memberEpoch: 0);

        Assert.Equal(ErrorCode.None, resp.ErrorCode);
        Assert.Equal("static-1", resp.MemberId);
    }

    [Fact]
    public void Heartbeat_MatchingEpoch_AdvancesNormally()
    {
        // Round-trip a normal heartbeat: the client echoes the broker's
        // last MemberEpoch. The fence must accept it (regression guard).
        var first = Heartbeat("g", "c1", memberId: "", memberEpoch: 0);
        var assignedId = first.MemberId!;
        var firstEpoch = first.MemberEpoch;

        var second = Heartbeat("g", "c1", memberId: assignedId, memberEpoch: firstEpoch);

        Assert.Equal(ErrorCode.None, second.ErrorCode);
        Assert.Equal(assignedId, second.MemberId);
    }

    private ConsumerGroupHeartbeatResponse Heartbeat(
        string group,
        string clientId,
        string memberId,
        int memberEpoch) =>
        _coordinator.HandleConsumerGroupHeartbeat(new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 0,
            ClientId = clientId,
            GroupId = group,
            MemberId = memberId,
            MemberEpoch = memberEpoch,
            SubscribedTopicNames = [Topic],
            RebalanceTimeoutMs = -1,
        });
}

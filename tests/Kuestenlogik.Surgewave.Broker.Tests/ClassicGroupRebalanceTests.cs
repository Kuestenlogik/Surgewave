using Kuestenlogik.Surgewave.Coordination.Consumer;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// The classic group protocol rebalances (#216). A member that joined later used to be added to the running
/// generation without anyone being told, so the first member kept every partition and the later one got none.
/// </summary>
public sealed class ClassicGroupRebalanceTests
{
    private const string Group = "orders-app";
    private const int SessionTimeoutMs = 10_000;
    private const int RebalanceTimeoutMs = 30_000;

    private readonly FakeTimeProvider _time = new();
    private readonly ConsumerGroupCoordinator _coordinator;
    private readonly CancellationToken _ct = TestContext.Current.CancellationToken;

    public ClassicGroupRebalanceTests()
    {
        _coordinator = new ConsumerGroupCoordinator(NullLogger<ConsumerGroupCoordinator>.Instance, timeProvider: _time);
    }

    [Fact]
    public async Task AMemberAloneFormsGenerationOneAndLeadsIt()
    {
        var join = await JoinAsync(string.Empty);

        Assert.Equal(ConsumerGroupErrorStatus.None, join.Status);
        Assert.Equal(1, join.GenerationId);
        Assert.Equal(join.MemberId, join.LeaderId);
        Assert.Single(join.Members);
    }

    [Fact]
    public async Task ASecondMemberStartsARebalanceThatBothJoin()
    {
        var first = await JoinAsync(string.Empty);
        await SyncAsync(first.MemberId, first.GenerationId, (first.MemberId, [1]));

        // The newcomer's join is held: the round waits for the member it already has.
        var secondJoin = JoinAsync(string.Empty);
        Assert.False(secondJoin.IsCompleted);

        // The first member learns of it from its heartbeat and rejoins; then the round completes for both.
        Assert.Equal(ConsumerGroupErrorStatus.RebalanceInProgress, Heartbeat(first.MemberId, first.GenerationId));
        var firstAgain = await JoinAsync(first.MemberId);
        var second = await secondJoin;

        Assert.Equal(2, firstAgain.GenerationId);
        Assert.Equal(2, second.GenerationId);
        Assert.Equal(first.MemberId, firstAgain.LeaderId);
        Assert.Equal(first.MemberId, second.LeaderId);
        Assert.Equal(2, firstAgain.Members.Count); // the leader sees both subscriptions
        Assert.Empty(second.Members);
    }

    [Fact]
    public async Task AFollowersSyncWaitsForTheLeadersAssignment()
    {
        var (leader, follower) = await TwoMembersAsync();

        var followerSync = SyncAsync(follower.MemberId, follower.GenerationId);
        Assert.False(followerSync.IsCompleted);

        var leaderSync = await SyncAsync(leader.MemberId, leader.GenerationId, (leader.MemberId, [1]), (follower.MemberId, [2]));

        Assert.Equal([1], leaderSync.Assignment);
        Assert.Equal([2], (await followerSync).Assignment);
        Assert.Equal(ConsumerGroupErrorStatus.None, Heartbeat(follower.MemberId, follower.GenerationId));
    }

    [Fact]
    public async Task AMemberThatDoesNotRejoinIsDroppedWhenTheRebalanceTimeoutPasses()
    {
        var (leader, follower) = await TwoMembersAsync();
        await SyncAsync(leader.MemberId, leader.GenerationId, (leader.MemberId, [1]), (follower.MemberId, [2]));

        var thirdJoin = JoinAsync(string.Empty);
        var leaderJoin = JoinAsync(leader.MemberId);
        Assert.False(thirdJoin.IsCompleted);

        // The follower heartbeats, so its session stays alive, but never rejoins: the rebalance timeout ends the wait.
        for (var elapsed = 0; elapsed < RebalanceTimeoutMs; elapsed += SessionTimeoutMs / 2)
        {
            Assert.False(thirdJoin.IsCompleted);
            Assert.Equal(ConsumerGroupErrorStatus.RebalanceInProgress, Heartbeat(follower.MemberId, follower.GenerationId));
            _time.Advance(TimeSpan.FromMilliseconds(SessionTimeoutMs / 2));
        }

        var third = await thirdJoin;
        var leaderAgain = await leaderJoin;
        Assert.Equal(3, third.GenerationId);
        Assert.Equal(2, leaderAgain.Members.Count);
        Assert.DoesNotContain(leaderAgain.Members, m => m.MemberId == follower.MemberId);
        Assert.Equal(ConsumerGroupErrorStatus.UnknownMember, Heartbeat(follower.MemberId, follower.GenerationId));
    }

    [Fact]
    public async Task AMemberWhoseSessionRunsOutLeavesAndTheRestRebalance()
    {
        var (leader, follower) = await TwoMembersAsync();
        await SyncAsync(leader.MemberId, leader.GenerationId, (leader.MemberId, [1]), (follower.MemberId, [2]));

        // The leader keeps heartbeating, the follower goes quiet.
        _time.Advance(TimeSpan.FromMilliseconds(SessionTimeoutMs / 2));
        Assert.Equal(ConsumerGroupErrorStatus.None, Heartbeat(leader.MemberId, leader.GenerationId));
        _time.Advance(TimeSpan.FromMilliseconds(SessionTimeoutMs / 2));

        Assert.Equal(ConsumerGroupErrorStatus.RebalanceInProgress, Heartbeat(leader.MemberId, leader.GenerationId));
        var alone = await JoinAsync(leader.MemberId);
        Assert.Equal(3, alone.GenerationId);
        Assert.Single(alone.Members);
    }

    [Fact]
    public async Task ALeavingMemberStartsARebalanceForTheOthers()
    {
        var (leader, follower) = await TwoMembersAsync();
        await SyncAsync(leader.MemberId, leader.GenerationId, (leader.MemberId, [1]), (follower.MemberId, [2]));

        var left = _coordinator.LeaveGroup(new LeaveGroupCommand { GroupId = Group, MemberId = follower.MemberId, Members = [] });
        Assert.Equal(ConsumerGroupErrorStatus.None, left.Status);

        Assert.Equal(ConsumerGroupErrorStatus.RebalanceInProgress, Heartbeat(leader.MemberId, leader.GenerationId));
        Assert.Single((await JoinAsync(leader.MemberId)).Members);
    }

    [Fact]
    public async Task AnOlderGenerationIsRefused()
    {
        var (leader, _) = await TwoMembersAsync();

        Assert.Equal(ConsumerGroupErrorStatus.IllegalGeneration, Heartbeat(leader.MemberId, leader.GenerationId - 1));
        Assert.Equal(ConsumerGroupErrorStatus.IllegalGeneration,
            (await SyncAsync(leader.MemberId, leader.GenerationId - 1)).Status);
    }

    [Fact]
    public async Task AJoinUnderAMemberIdTheGroupDoesNotKnowIsRefused()
    {
        await JoinAsync(string.Empty);

        var join = await JoinAsync("gone-member");

        Assert.Equal(ConsumerGroupErrorStatus.UnknownMember, join.Status);
        Assert.Equal(-1, join.GenerationId);
    }

    [Fact]
    public async Task AJoinWhoseConnectionCloses_DoesNotCountAsRejoined()
    {
        var first = await JoinAsync(string.Empty);
        await SyncAsync(first.MemberId, first.GenerationId, (first.MemberId, [1]));

        using var gone = new CancellationTokenSource();
        var abandoned = _coordinator.JoinGroupAsync(Command(string.Empty), gone.Token);
        await gone.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned);

        // The first member rejoins; the abandoned newcomer is still a member that has not, so the round waits —
        // until the newcomer's session runs out.
        var rejoin = JoinAsync(first.MemberId);
        Assert.False(rejoin.IsCompleted);
        _time.Advance(TimeSpan.FromMilliseconds(SessionTimeoutMs));
        Assert.Single((await rejoin).Members);
    }

    private async Task<(JoinGroupResult Leader, JoinGroupResult Follower)> TwoMembersAsync()
    {
        var first = await JoinAsync(string.Empty);
        var secondJoin = JoinAsync(string.Empty);
        var leader = await JoinAsync(first.MemberId);
        var follower = await secondJoin;
        Assert.Equal(leader.GenerationId, follower.GenerationId);
        return (leader, follower);
    }

    private Task<JoinGroupResult> JoinAsync(string memberId) => _coordinator.JoinGroupAsync(Command(memberId), _ct);

    private static JoinGroupCommand Command(string memberId) => new()
    {
        GroupId = Group,
        MemberId = memberId,
        ClientId = "test",
        ProtocolType = "consumer",
        SessionTimeoutMs = SessionTimeoutMs,
        RebalanceTimeoutMs = RebalanceTimeoutMs,
        Protocols = [new GroupJoinProtocol("range", [])],
    };

    private Task<SyncGroupResult> SyncAsync(string memberId, int generation, params (string MemberId, byte[] Assignment)[] assignments) =>
        _coordinator.SyncGroupAsync(new SyncGroupCommand
        {
            GroupId = Group,
            MemberId = memberId,
            GenerationId = generation,
            Assignments = [.. assignments.Select(a => new SyncGroupAssignmentInput(a.MemberId, a.Assignment))],
        }, _ct);

    private ConsumerGroupErrorStatus Heartbeat(string memberId, int generation) =>
        _coordinator.Heartbeat(new GroupHeartbeatCommand(Group, memberId, generation)).Status;
}

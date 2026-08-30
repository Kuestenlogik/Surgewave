using System.Threading;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Core.Observability;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tap-side observability coverage: when the coordinator is given a
/// <see cref="SurgewaveBrokerObservability"/> instance it must publish a
/// <see cref="SurgewaveBrokerEventKind.Rebalanced"/> on the leader's
/// SyncGroup (i.e. the moment the group's ownership changes). These
/// tests exercise the protocol-neutral coordinator surface directly (#59)
/// so they don't need the full broker pipeline standing up.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ObservabilityWiringTests
{
    [Fact]
    public async Task LeaderSyncGroup_PublishesRebalancedEvent()
    {
        var observability = new SurgewaveBrokerObservability(
            NullLogger<SurgewaveBrokerObservability>.Instance);
        var coordinator = new ConsumerGroupCoordinator(
            NullLogger<ConsumerGroupCoordinator>.Instance,
            offsetStore: null, logManager: null, aclAuthorizer: null,
            observability: observability);

        var memberId = JoinAndGetMemberId(coordinator, "g1");

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var reader = ReadFirstAsync(observability, cts.Token);

        // Leader distributes assignments — this is the "rebalance done" tick.
        var sync = coordinator.SyncGroup(new SyncGroupCommand
        {
            GroupId = "g1",
            GenerationId = 1,
            MemberId = memberId,
            Assignments =
            [
                new SyncGroupAssignmentInput(memberId, [0x01, 0x02, 0x03]),
            ],
        });
        Assert.Equal(ConsumerGroupErrorStatus.None, sync.Status);

        var ev = await reader;
        Assert.Equal(SurgewaveBrokerEventKind.Rebalanced, ev.Kind);
        Assert.Equal(-1, ev.Partition);
        Assert.NotNull(ev.Consumers);
        Assert.Equal("g1", ev.Consumers![0]);
    }

    [Fact]
    public async Task FollowerSyncGroup_DoesNotPublish()
    {
        var observability = new SurgewaveBrokerObservability(
            NullLogger<SurgewaveBrokerObservability>.Instance);
        var coordinator = new ConsumerGroupCoordinator(
            NullLogger<ConsumerGroupCoordinator>.Instance,
            offsetStore: null, logManager: null, aclAuthorizer: null,
            observability: observability);

        var memberId = JoinAndGetMemberId(coordinator, "g2");

        // Follower path: Assignments empty — coordinator just returns
        // whatever's stored, no new rebalance signal should fire.
        var sync = coordinator.SyncGroup(new SyncGroupCommand
        {
            GroupId = "g2",
            GenerationId = 1,
            MemberId = memberId,
            Assignments = [],
        });
        Assert.Equal(ConsumerGroupErrorStatus.None, sync.Status);

        // ThrowsAnyAsync, not ThrowsAsync: the assertion is "nothing was published, so the read
        // waits until it is cancelled". Which cancellation type surfaces —
        // OperationCanceledException or the TaskCanceledException derived from it — is the
        // runtime's choice, and it changed under a servicing update. Pinning the exact type was
        // pinning an implementation detail rather than the behaviour.
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ReadFirstAsync(observability, cts.Token));
    }

    private static string JoinAndGetMemberId(ConsumerGroupCoordinator coordinator, string groupId)
    {
        var join = coordinator.JoinGroup(new JoinGroupCommand
        {
            GroupId = groupId,
            MemberId = string.Empty,
            ClientId = "test",
            ProtocolType = "consumer",
            SessionTimeoutMs = 10_000,
            Protocols = [new GroupJoinProtocol("range", [])],
        });
        Assert.NotEmpty(join.MemberId);
        return join.MemberId;
    }

    private static async Task<SurgewaveBrokerEvent> ReadFirstAsync(
        ISurgewaveBrokerObservability observability, CancellationToken ct)
    {
        await foreach (var ev in observability.ObserveAsync(ct).ConfigureAwait(false))
            return ev;
        throw new ChannelClosedException("stream closed without publishing");
    }
}

using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Per-leader admission and backoff in the replica fetch loop (#122, last part).
///
/// <para>The loop used to await <c>Task.WhenAll</c> over every leader, so each round was only as
/// fast as its slowest peer. A leader that was simply gone burned three connection attempts with
/// backoff inside that await, and every healthy leader on the same broker stopped replicating for
/// the duration — replication lag inherited from an unrelated partition.</para>
///
/// <para>The gate replaces the join as the thing that bounds concurrency: one fetch in flight per
/// leader, and a failing leader backs off instead of paying the connection cost every interval.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class LeaderFetchGateTests
{
    private static readonly TimeSpan FetchInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    [Fact]
    public void OnlyOneFetchPerLeaderIsAdmittedAtATime()
    {
        var gate = new ReplicaFetcher.LeaderFetchGate();

        Assert.True(gate.TryEnter());

        // This is what lets the loop dispatch without awaiting: a leader that is still busy is
        // skipped rather than joined on.
        Assert.False(gate.TryEnter());

        gate.Exit();
        Assert.True(gate.TryEnter());
    }

    [Fact]
    public void AHealthyLeaderIsNeverDelayed()
    {
        var gate = new ReplicaFetcher.LeaderFetchGate();
        gate.RecordFailure(FetchInterval, MaxBackoff);
        Assert.True(gate.NextAttemptUtc > DateTimeOffset.UtcNow);

        gate.RecordSuccess();

        // Back in the rotation immediately — a peer that recovers must not sit out its backoff.
        Assert.True(gate.NextAttemptUtc <= DateTimeOffset.UtcNow);
    }

    [Fact]
    public void RepeatedFailuresBackOffFurtherEachTime()
    {
        var gate = new ReplicaFetcher.LeaderFetchGate();

        gate.RecordFailure(FetchInterval, MaxBackoff);
        var first = gate.NextAttemptUtc;

        gate.RecordFailure(FetchInterval, MaxBackoff);
        var second = gate.NextAttemptUtc;

        Assert.True(second > first, $"expected growing backoff, got {first:o} then {second:o}");
    }

    [Fact]
    public void BackoffIsCapped()
    {
        var gate = new ReplicaFetcher.LeaderFetchGate();

        // A peer that has been gone for a long time must still be retried on a bounded schedule,
        // otherwise it is never noticed coming back.
        for (var i = 0; i < 50; i++)
            gate.RecordFailure(FetchInterval, MaxBackoff);

        var delay = gate.NextAttemptUtc - DateTimeOffset.UtcNow;

        Assert.True(delay <= MaxBackoff, $"backoff {delay} exceeded the {MaxBackoff} cap");
        Assert.True(delay > TimeSpan.Zero);
    }

    [Fact]
    public void AFreshGateIsImmediatelyEligible()
    {
        var gate = new ReplicaFetcher.LeaderFetchGate();

        Assert.True(gate.NextAttemptUtc <= DateTimeOffset.UtcNow);
    }
}

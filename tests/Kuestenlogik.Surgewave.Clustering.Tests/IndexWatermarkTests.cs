using System.Runtime.CompilerServices;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The signal <see cref="RaftNode.WaitForCommitIndexAsync"/> and <see cref="RaftNode.WaitForCommitAsync"/> wait on
/// (#201): a wait completes when the index is reached, at once if it already was, ends with its token, and leaves no
/// waiter behind either way.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class IndexWatermarkTests
{
    [Fact]
    public void WaitAsync_IndexAlreadyReached_CompletesAtOnce()
    {
        var watermark = new IndexWatermark();
        watermark.Advance(5);

        Assert.True(watermark.WaitAsync(5, CancellationToken.None).IsCompletedSuccessfully);
        Assert.True(watermark.WaitAsync(3, CancellationToken.None).IsCompletedSuccessfully);
        Assert.Equal(0, watermark.WaiterCount);
    }

    [Fact]
    public void WaitAsync_IndexAlreadyReached_CompletesEvenWithACancelledToken()
    {
        var watermark = new IndexWatermark(initial: 7);

        Assert.True(watermark.WaitAsync(7, new CancellationToken(canceled: true)).IsCompletedSuccessfully);
    }

    [Fact]
    public async Task WaitAsync_IndexReachedLater_CompletesOnTheAdvanceThatReachesIt()
    {
        var watermark = new IndexWatermark();
        var wait = watermark.WaitAsync(3, TestContext.Current.CancellationToken);

        watermark.Advance(2);
        Assert.False(wait.IsCompleted);
        Assert.Equal(1, watermark.WaiterCount);

        watermark.Advance(3);
        await wait;
        Assert.Equal(0, watermark.WaiterCount);
    }

    [Fact]
    public async Task Advance_PastSeveralWaiters_CompletesThoseReachedAndKeepsTheRest()
    {
        var watermark = new IndexWatermark();
        var ct = TestContext.Current.CancellationToken;
        var first = watermark.WaitAsync(1, ct);
        var second = watermark.WaitAsync(4, ct);
        var third = watermark.WaitAsync(9, ct);

        watermark.Advance(5);

        await first;
        await second;
        Assert.False(third.IsCompleted);
        Assert.Equal(1, watermark.WaiterCount);

        watermark.Advance(9);
        await third;
        Assert.Equal(0, watermark.WaiterCount);
    }

    [Fact]
    public void Advance_Backwards_IsIgnored()
    {
        var watermark = new IndexWatermark();
        watermark.Advance(8);
        watermark.Advance(4);

        Assert.Equal(8, watermark.Value);
        Assert.True(watermark.WaitAsync(8, CancellationToken.None).IsCompletedSuccessfully);
    }

    [Fact]
    public async Task WaitAsync_TokenCancelled_EndsCancelledAndRemovesTheWaiter()
    {
        var watermark = new IndexWatermark();
        using var cts = new CancellationTokenSource();
        var wait = watermark.WaitAsync(10, cts.Token);
        Assert.Equal(1, watermark.WaiterCount);

        await cts.CancelAsync();

        var cancelled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
        Assert.Equal(cts.Token, cancelled.CancellationToken);
        Assert.Equal(0, watermark.WaiterCount);

        // A later advance finds nothing left to complete.
        watermark.Advance(10);
        Assert.Equal(0, watermark.WaiterCount);
    }

    [Fact]
    public async Task WaitAsync_TokenAlreadyCancelled_EndsCancelledWithoutAWaiter()
    {
        var watermark = new IndexWatermark();
        var cancelled = new CancellationToken(canceled: true);

        var wait = watermark.WaitAsync(1, cancelled);

        Assert.True(wait.IsCanceled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
        Assert.Equal(0, watermark.WaiterCount);
    }

    [Fact]
    public void WaitAsync_CompletedByAdvance_ReleasesItsCancellationRegistration()
    {
        // The registration is the leak that would outlive the wait: a long-lived token (a broker's lifetime) would
        // keep every completed wait reachable through its callback. With the registration released, nothing holds
        // the completed wait once its caller lets go.
        var watermark = new IndexWatermark();
        using var cts = new CancellationTokenSource();

        var wait = WaitAndComplete(watermark, cts.Token);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.False(wait.IsAlive, "the token still references a completed wait");
        Assert.Equal(0, watermark.WaiterCount);
        GC.KeepAlive(cts);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference WaitAndComplete(IndexWatermark watermark, CancellationToken ct)
    {
        var wait = watermark.WaitAsync(2, ct);
        watermark.Advance(2);
        Assert.True(wait.IsCompletedSuccessfully);
        return new WeakReference(wait);
    }

    [Fact]
    public async Task AdvanceAndCancel_Racing_EveryWaitEndsAndNoneIsLeft()
    {
        var watermark = new IndexWatermark();
        var waits = new List<Task>();
        var sources = new List<CancellationTokenSource>();
        for (var i = 1; i <= 200; i++)
        {
            var cts = new CancellationTokenSource();
            sources.Add(cts);
            waits.Add(watermark.WaitAsync(i, cts.Token));
        }

        var cancelling = Task.Run(async () =>
        {
            foreach (var cts in sources)
                await cts.CancelAsync();
        }, TestContext.Current.CancellationToken);
        var advancing = Task.Run(() =>
        {
            for (var i = 1; i <= 200; i++)
                watermark.Advance(i);
        }, TestContext.Current.CancellationToken);
        await Task.WhenAll(cancelling, advancing);

        foreach (var wait in waits)
        {
            // Each one either reached its index or was cancelled; none hangs.
            try { await wait; }
            catch (OperationCanceledException) { }
        }

        Assert.Equal(0, watermark.WaiterCount);
        foreach (var cts in sources)
            cts.Dispose();
    }
}

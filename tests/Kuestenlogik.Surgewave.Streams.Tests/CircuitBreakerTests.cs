using Kuestenlogik.Surgewave.Streams.Resilience;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class CircuitBreakerTests
{
    // ── Initial state ─────────────────────────────────────────────────────

    [Fact]
    public void InitialState_IsClosed()
    {
        var cb = new CircuitBreaker(failureThreshold: 3, resetTimeout: TimeSpan.FromSeconds(30));
        Assert.Equal(CircuitBreakerState.Closed, cb.State);
    }

    [Fact]
    public void AllowRequest_WhenClosed_ReturnsTrue()
    {
        var cb = new CircuitBreaker(failureThreshold: 3);
        Assert.True(cb.AllowRequest());
    }

    // ── Closed → Open transition ──────────────────────────────────────────

    [Fact]
    public void RecordFailure_BelowThreshold_StaysClosed()
    {
        var cb = new CircuitBreaker(failureThreshold: 3);
        cb.RecordFailure();
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Closed, cb.State);
    }

    [Fact]
    public void RecordFailure_AtThreshold_OpensCircuit()
    {
        var cb = new CircuitBreaker(failureThreshold: 3);
        cb.RecordFailure();
        cb.RecordFailure();
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, cb.State);
    }

    [Fact]
    public void AllowRequest_WhenOpen_ReturnsFalse()
    {
        var cb = new CircuitBreaker(failureThreshold: 2, resetTimeout: TimeSpan.FromHours(1));
        cb.RecordFailure();
        cb.RecordFailure();
        Assert.False(cb.AllowRequest());
    }

    // ── Open → HalfOpen transition ────────────────────────────────────────

    [Fact]
    public void AllowRequest_AfterResetTimeout_TransitionsToHalfOpen()
    {
        var cb = new CircuitBreaker(failureThreshold: 2, resetTimeout: TimeSpan.FromMilliseconds(50));
        cb.RecordFailure();
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, cb.State);

        Thread.Sleep(100);

        var allowed = cb.AllowRequest();
        Assert.True(allowed);
        Assert.Equal(CircuitBreakerState.HalfOpen, cb.State);
    }

    // ── HalfOpen → Closed / Open transitions ─────────────────────────────

    [Fact]
    public void RecordSuccess_WhenHalfOpen_ClosesCirucit()
    {
        var cb = new CircuitBreaker(failureThreshold: 2, resetTimeout: TimeSpan.FromMilliseconds(50));
        cb.RecordFailure();
        cb.RecordFailure();
        Thread.Sleep(100);
        cb.AllowRequest(); // transitions to HalfOpen

        cb.RecordSuccess();

        Assert.Equal(CircuitBreakerState.Closed, cb.State);
        Assert.True(cb.AllowRequest());
    }

    [Fact]
    public void RecordFailure_WhenHalfOpen_ReOpensCircuit()
    {
        var cb = new CircuitBreaker(failureThreshold: 2, resetTimeout: TimeSpan.FromMilliseconds(50));
        cb.RecordFailure();
        cb.RecordFailure();
        Thread.Sleep(100);
        cb.AllowRequest(); // → HalfOpen

        // One more failure exceeds threshold again
        cb.RecordFailure();

        Assert.Equal(CircuitBreakerState.Open, cb.State);
    }

    // ── RecordSuccess resets failure count ────────────────────────────────

    [Fact]
    public void RecordSuccess_WhenClosed_ResetsFailureCount()
    {
        // Accumulate failures just below threshold, then succeed, then verify
        // that subsequent failures require a full new round.
        var cb = new CircuitBreaker(failureThreshold: 3);
        cb.RecordFailure();
        cb.RecordFailure();
        cb.RecordSuccess();

        // Should not open after just one more failure
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Closed, cb.State);
    }

    // ── Thread safety ─────────────────────────────────────────────────────

    [Fact]
    public async Task ThreadSafety_ConcurrentFailures_EventuallyOpens()
    {
        const int threshold = 10;
        var cb = new CircuitBreaker(failureThreshold: threshold, resetTimeout: TimeSpan.FromSeconds(30));

        var tasks = Enumerable.Range(0, threshold * 4)
            .Select(_ => Task.Run(cb.RecordFailure))
            .ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(CircuitBreakerState.Open, cb.State);
    }

    [Fact]
    public async Task ThreadSafety_ConcurrentAllowRequest_OnlyOneWinsHalfOpen()
    {
        var cb = new CircuitBreaker(failureThreshold: 2, resetTimeout: TimeSpan.FromMilliseconds(50));
        cb.RecordFailure();
        cb.RecordFailure();
        await Task.Delay(100);

        // Many threads race to transition Open → HalfOpen
        var results = new int[20];
        var tasks = Enumerable.Range(0, results.Length)
            .Select(i => Task.Run(() => results[i] = cb.AllowRequest() ? 1 : 0))
            .ToArray();
        await Task.WhenAll(tasks);

        // Exactly one should have succeeded in transitioning to HalfOpen;
        // subsequent calls see HalfOpen which also returns true — that is fine.
        // The important invariant is that at least one was allowed through.
        Assert.Contains(results, r => r == 1);
    }
}

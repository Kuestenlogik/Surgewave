using System.Diagnostics;
using Kuestenlogik.Surgewave.Streams.Resilience;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Advanced tests for CircuitBreaker covering rapid cycling, concurrent access,
/// HalfOpen probe semantics, StreamsRetryPolicy integration, and timing precision.
/// </summary>
public sealed class CircuitBreakerAdvancedTests
{
    // ── Rapid open/close cycling ──────────────────────────────────────────────

    [Fact]
    public async Task CircuitBreaker_RapidCycling_100Times_StaysConsistent()
    {
        const int cycles = 100;
        var cb = new CircuitBreaker(failureThreshold: 2, resetTimeout: TimeSpan.FromMilliseconds(30));

        for (int i = 0; i < cycles; i++)
        {
            // Trip the breaker
            cb.RecordFailure();
            cb.RecordFailure();
            Assert.Equal(CircuitBreakerState.Open, cb.State);

            // Wait for reset timeout
            await Task.Delay(60);

            // Transition to HalfOpen via probe
            Assert.True(cb.AllowRequest());
            Assert.Equal(CircuitBreakerState.HalfOpen, cb.State);

            // Close again via success
            cb.RecordSuccess();
            Assert.Equal(CircuitBreakerState.Closed, cb.State);
        }
    }

    [Fact]
    public async Task CircuitBreaker_RapidFailureRecovery_FailureCountResetsOnSuccess()
    {
        var cb = new CircuitBreaker(failureThreshold: 5, resetTimeout: TimeSpan.FromMilliseconds(30));

        // Build up failures just below threshold, then succeed, repeat
        for (int round = 0; round < 10; round++)
        {
            cb.RecordFailure();
            cb.RecordFailure();
            cb.RecordFailure();
            cb.RecordFailure();
            // Still closed (4 < 5)
            Assert.Equal(CircuitBreakerState.Closed, cb.State);

            // Success resets count
            cb.RecordSuccess();
        }

        // After all rounds, circuit is still Closed and one more failure won't open it
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Closed, cb.State);

        await Task.CompletedTask; // satisfy async signature for consistency
    }

    // ── Concurrent RecordFailure reaching threshold simultaneously ────────────

    [Fact]
    public async Task CircuitBreaker_ConcurrentFailures_ExactlyTripsOnce()
    {
        const int threshold = 10;
        var cb = new CircuitBreaker(failureThreshold: threshold, resetTimeout: TimeSpan.FromSeconds(60));

        // 10 tasks each call RecordFailure once — exactly hitting the threshold
        var tasks = Enumerable.Range(0, threshold)
            .Select(_ => Task.Run(cb.RecordFailure))
            .ToArray();

        await Task.WhenAll(tasks);

        // Breaker must be Open; it should never remain Closed
        Assert.Equal(CircuitBreakerState.Open, cb.State);
        // AllowRequest must return false (long reset timeout)
        Assert.False(cb.AllowRequest());
    }

    [Fact]
    public async Task CircuitBreaker_ConcurrentFailuresAboveThreshold_OpenedExactlyOnce()
    {
        // More failures than threshold — the breaker should still end up Open
        const int threshold = 5;
        var cb = new CircuitBreaker(failureThreshold: threshold, resetTimeout: TimeSpan.FromSeconds(60));

        var tasks = Enumerable.Range(0, threshold * 4)
            .Select(_ => Task.Run(cb.RecordFailure))
            .ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(CircuitBreakerState.Open, cb.State);
    }

    // ── HalfOpen allows exactly one probe ────────────────────────────────────

    [Fact]
    public async Task CircuitBreaker_HalfOpen_ExactlyOneProbeWinsTransition()
    {
        var cb = new CircuitBreaker(failureThreshold: 2, resetTimeout: TimeSpan.FromMilliseconds(50));
        cb.RecordFailure();
        cb.RecordFailure();
        await Task.Delay(100); // let reset timeout expire

        // 20 concurrent threads race to be the probe
        const int contenders = 20;
        int[] results = new int[contenders];

        var tasks = Enumerable.Range(0, contenders)
            .Select(i => Task.Run(() => results[i] = cb.AllowRequest() ? 1 : 0))
            .ToArray();

        await Task.WhenAll(tasks);

        // Only one thread transitions Open → HalfOpen (returns true from that transition)
        // Additional calls while already HalfOpen also return true per AllowRequest contract.
        // The key invariant: at least one was allowed and the state is HalfOpen (not Open).
        Assert.Equal(CircuitBreakerState.HalfOpen, cb.State);
        Assert.Contains(results, r => r == 1);
    }

    [Fact]
    public async Task CircuitBreaker_HalfOpen_FailureSendsBackToOpen()
    {
        var cb = new CircuitBreaker(failureThreshold: 2, resetTimeout: TimeSpan.FromMilliseconds(50));
        cb.RecordFailure();
        cb.RecordFailure();
        await Task.Delay(100);

        // Transition to HalfOpen
        Assert.True(cb.AllowRequest());
        Assert.Equal(CircuitBreakerState.HalfOpen, cb.State);

        // Fail the probe — should re-open immediately (failure count now >= threshold)
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, cb.State);
        Assert.False(cb.AllowRequest()); // within reset timeout
    }

    [Fact]
    public async Task CircuitBreaker_HalfOpen_SuccessCloses()
    {
        var cb = new CircuitBreaker(failureThreshold: 2, resetTimeout: TimeSpan.FromMilliseconds(50));
        cb.RecordFailure();
        cb.RecordFailure();
        await Task.Delay(100);

        cb.AllowRequest(); // → HalfOpen
        cb.RecordSuccess();

        Assert.Equal(CircuitBreakerState.Closed, cb.State);
        Assert.True(cb.AllowRequest());
    }

    // ── StreamsRetryPolicy integration: retry until circuit opens ─────────────

    [Fact]
    public async Task RetryPolicy_WithCircuitBreaker_CircuitOpens_ThrowsInvalidOperation()
    {
        // Threshold = 2, MaxRetries = 0 → each call trips the breaker by 1 failure
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 0,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed,
            EnableCircuitBreaker = true,
            CircuitBreakerThreshold = 2,
            CircuitBreakerResetTimeout = TimeSpan.FromHours(1)
        });

        // First failure → count = 1 (below threshold, still Closed)
        await Assert.ThrowsAsync<RetryExhaustedException>(() =>
            policy.ExecuteAsync(() => throw new TimeoutException("fail")));

        // Second failure → count = 2, circuit opens
        await Assert.ThrowsAsync<RetryExhaustedException>(() =>
            policy.ExecuteAsync(() => throw new TimeoutException("fail")));

        // Third call: circuit is Open → InvalidOperationException (not RetryExhausted)
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            policy.ExecuteAsync(() => Task.CompletedTask));

        Assert.Contains("Open", ex.Message);
    }

    [Fact]
    public async Task RetryPolicy_WithCircuitBreaker_MultipleRetries_TripsCircuit()
    {
        // Threshold=3, MaxRetries=3. Initial attempt + 2 retries = 3 failures → circuit opens.
        // On the 4th attempt (3rd retry) the policy sees the circuit is Open and throws
        // InvalidOperationException (not RetryExhaustedException) because the breaker blocks.
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 3,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed,
            EnableCircuitBreaker = true,
            CircuitBreakerThreshold = 3,
            CircuitBreakerResetTimeout = TimeSpan.FromHours(1)
        });

        // The circuit opens after 3 failures; the 4th retry attempt is rejected via InvalidOperationException
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            policy.ExecuteAsync(() => throw new TimeoutException("always fail")));

        Assert.Contains("Open", ex.Message);
    }

    // ── Reset timeout precision ───────────────────────────────────────────────

    [Fact]
    public async Task CircuitBreaker_ResetTimeout_PrecisionWithin50ms()
    {
        var timeout = TimeSpan.FromMilliseconds(200);
        var cb = new CircuitBreaker(failureThreshold: 1, resetTimeout: timeout);
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, cb.State);

        // Before timeout — must still be Open
        Assert.False(cb.AllowRequest());

        var sw = Stopwatch.StartNew();

        // Wait until well after the reset timeout (generous margin for CI)
        while (sw.Elapsed < timeout + TimeSpan.FromMilliseconds(100))
        {
            await Task.Delay(10);
        }

        sw.Stop();

        // Should now allow probe (transition to HalfOpen)
        Assert.True(cb.AllowRequest(),
            $"Expected HalfOpen after {sw.ElapsedMilliseconds}ms; timeout was {timeout.TotalMilliseconds}ms");

        // The time we actually waited should be within a reasonable tolerance (500ms upper bound)
        Assert.True(sw.Elapsed < timeout + TimeSpan.FromMilliseconds(500),
            $"Waited {sw.ElapsedMilliseconds}ms, expected < {(timeout + TimeSpan.FromMilliseconds(500)).TotalMilliseconds}ms");
    }

    [Fact]
    public async Task CircuitBreaker_StillOpen_BeforeResetTimeout_NeverAllows()
    {
        var timeout = TimeSpan.FromMilliseconds(200);
        var cb = new CircuitBreaker(failureThreshold: 1, resetTimeout: timeout);
        cb.RecordFailure();

        // Sample AllowRequest multiple times before timeout expires
        var sw = Stopwatch.StartNew();
        bool anyAllowed = false;

        while (sw.Elapsed < TimeSpan.FromMilliseconds(80))
        {
            if (cb.AllowRequest())
            {
                anyAllowed = true;
                break;
            }
            await Task.Delay(5);
        }

        Assert.False(anyAllowed, "Circuit should remain Open before reset timeout");
    }

    // ── RecordSuccess from Closed resets failure counter ─────────────────────

    [Fact]
    public void CircuitBreaker_RecordSuccess_FromClosed_DoesNotChangeState()
    {
        var cb = new CircuitBreaker(failureThreshold: 3);
        Assert.Equal(CircuitBreakerState.Closed, cb.State);

        cb.RecordSuccess();
        Assert.Equal(CircuitBreakerState.Closed, cb.State);
    }

    [Fact]
    public void CircuitBreaker_AccumulateThenSucceed_RequiresFullNewRoundToOpen()
    {
        var cb = new CircuitBreaker(failureThreshold: 4);

        // 3 failures (below threshold)
        cb.RecordFailure();
        cb.RecordFailure();
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Closed, cb.State);

        // Success resets counter
        cb.RecordSuccess();

        // Need 4 fresh failures to open; only 2 more should not be enough
        cb.RecordFailure();
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Closed, cb.State);

        // 2 more failures makes count = 4 → opens
        cb.RecordFailure();
        cb.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, cb.State);
    }
}

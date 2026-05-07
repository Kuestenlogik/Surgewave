using Kuestenlogik.Surgewave.Streams.Resilience;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class RetryAdvancedTests
{
    // ── ExecuteAsync – basic async retry ──────────────────────────────────

    [Fact]
    public async Task ExecuteAsync_SucceedsOnFirstAttempt_NoRetry()
    {
        var called = 0;
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 3,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed
        });

        await policy.ExecuteAsync(() => { called++; return Task.CompletedTask; });

        Assert.Equal(1, called);
    }

    [Fact]
    public async Task ExecuteAsync_TransientFailure_RetriesAndSucceeds()
    {
        var attempt = 0;
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 3,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed
        });

        await policy.ExecuteAsync(() =>
        {
            attempt++;
            if (attempt < 3)
                throw new TimeoutException("transient");
            return Task.CompletedTask;
        });

        Assert.Equal(3, attempt);
    }

    [Fact]
    public async Task ExecuteAsync_MetricsRecorded()
    {
        var attempt = 0;
        using var metrics = new StreamsMetrics();
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 3,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed
        });

        await policy.ExecuteAsync(() =>
        {
            attempt++;
            if (attempt < 3)
                throw new TimeoutException("transient");
            return Task.CompletedTask;
        }, metrics);

        Assert.Equal(2, metrics.Retries);
    }

    // ── ExecuteAsync – RetryExhaustedException ────────────────────────────

    [Fact]
    public async Task ExecuteAsync_AllAttemptsFail_ThrowsRetryExhaustedException()
    {
        const int maxRetries = 2;
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = maxRetries,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed
        });

        var ex = await Assert.ThrowsAsync<RetryExhaustedException>(() =>
            policy.ExecuteAsync(() => throw new TimeoutException("always fails")));

        Assert.NotNull(ex.InnerException);
        Assert.IsType<TimeoutException>(ex.InnerException);
        Assert.Equal(maxRetries + 1, ex.Attempts); // initial + retries
        Assert.True(ex.Elapsed >= TimeSpan.Zero);
    }

    [Fact]
    public async Task ExecuteAsync_RetryExhausted_ContainsElapsedTime()
    {
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 2,
            InitialDelay = TimeSpan.FromMilliseconds(10),
            BackoffStrategy = BackoffStrategy.Fixed
        });

        var ex = await Assert.ThrowsAsync<RetryExhaustedException>(() =>
            policy.ExecuteAsync(() => throw new TimeoutException("fail")));

        // At least two 10 ms delays should have elapsed
        Assert.True(ex.Elapsed.TotalMilliseconds >= 10,
            $"Expected elapsed ≥ 10 ms but got {ex.Elapsed.TotalMilliseconds:F0} ms");
    }

    [Fact]
    public async Task ExecuteAsync_NonRetryableException_ThrowsRetryExhaustedImmediately()
    {
        var attempt = 0;
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 5,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed,
            ShouldRetry = ex => ex is not ArgumentException
        });

        var rex = await Assert.ThrowsAsync<RetryExhaustedException>(() =>
            policy.ExecuteAsync(() =>
            {
                attempt++;
                throw new ArgumentException("non-retryable");
            }));

        // Non-retryable: only one attempt, no retry loop
        Assert.Equal(1, attempt);
        Assert.IsType<ArgumentException>(rex.InnerException);
    }

    // ── ExecuteAsync – CancellationToken ──────────────────────────────────

    [Fact]
    public async Task ExecuteAsync_CancellationRequested_ThrowsOperationCanceled()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 3,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed
        });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            policy.ExecuteAsync(() => Task.CompletedTask, cancellationToken: cts.Token));
    }

    // ── Circuit breaker integration ───────────────────────────────────────

    [Fact]
    public async Task ExecuteAsync_CircuitBreakerTrips_RejectsSubsequentRequests()
    {
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 0,               // no retries so the breaker trips fast
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed,
            EnableCircuitBreaker = true,
            CircuitBreakerThreshold = 2,
            CircuitBreakerResetTimeout = TimeSpan.FromHours(1) // never resets during the test
        });

        // Trip the breaker: 2 failures (each call is 1 attempt, MaxRetries=0 means no retry)
        for (var i = 0; i < 2; i++)
        {
            await Assert.ThrowsAsync<RetryExhaustedException>(() =>
                policy.ExecuteAsync(() => throw new TimeoutException("fail")));
        }

        // Now the circuit is Open; next call should be rejected
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            policy.ExecuteAsync(() => Task.CompletedTask));

        Assert.Contains("Open", ex.Message);
    }

    [Fact]
    public async Task ExecuteAsync_CircuitBreakerResets_AfterTimeout()
    {
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 0,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed,
            EnableCircuitBreaker = true,
            CircuitBreakerThreshold = 2,
            CircuitBreakerResetTimeout = TimeSpan.FromMilliseconds(60)
        });

        // Trip
        for (var i = 0; i < 2; i++)
            await Assert.ThrowsAsync<RetryExhaustedException>(() =>
                policy.ExecuteAsync(() => throw new TimeoutException("fail")));

        // Wait for reset timeout
        await Task.Delay(120);

        // A successful call should now be allowed and close the breaker
        var called = false;
        await policy.ExecuteAsync(() => { called = true; return Task.CompletedTask; });
        Assert.True(called);
    }

    [Fact]
    public async Task ExecuteAsync_CircuitBreakerClosedByDefault_AllowsRequests()
    {
        var policy = new StreamsRetryPolicy(new StreamsRetryConfig
        {
            MaxRetries = 3,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            BackoffStrategy = BackoffStrategy.Fixed,
            EnableCircuitBreaker = true,
            CircuitBreakerThreshold = 10,
            CircuitBreakerResetTimeout = TimeSpan.FromSeconds(30)
        });

        var called = false;
        await policy.ExecuteAsync(() => { called = true; return Task.CompletedTask; });
        Assert.True(called);
    }

    // ── RetryExhaustedException properties ────────────────────────────────

    [Fact]
    public void RetryExhaustedException_PropertiesAreSet()
    {
        var inner = new TimeoutException("boom");
        var elapsed = TimeSpan.FromSeconds(1.5);

        var ex = new RetryExhaustedException(4, elapsed, inner);

        Assert.Equal(4, ex.Attempts);
        Assert.Equal(elapsed, ex.Elapsed);
        Assert.Same(inner, ex.InnerException);
        Assert.Contains("4", ex.Message);
    }
}

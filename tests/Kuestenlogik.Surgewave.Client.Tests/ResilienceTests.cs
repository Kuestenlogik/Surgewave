using Kuestenlogik.Surgewave.Client.Resilience;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for the resilience patterns (Circuit Breaker, Retry Policy, Bulkhead, Pipeline).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ResilienceTests
{
    #region Circuit Breaker Tests

    [Fact]
    public void CircuitBreaker_StartsInClosedState()
    {
        var cb = new CircuitBreaker();
        Assert.Equal(CircuitBreakerState.Closed, cb.State);
    }

    [Fact]
    public void CircuitBreaker_OpensAfterFailureThreshold()
    {
        var config = new CircuitBreakerConfig { FailureThreshold = 3 };
        var cb = new CircuitBreaker(config);

        // Cause 3 failures
        for (int i = 0; i < 3; i++)
        {
            Assert.Throws<InvalidOperationException>(() =>
                cb.Execute<int>(() => throw new InvalidOperationException("Test error")));
        }

        Assert.Equal(CircuitBreakerState.Open, cb.State);
    }

    [Fact]
    public void CircuitBreaker_RejectsWhenOpen()
    {
        var cb = new CircuitBreaker(new CircuitBreakerConfig { FailureThreshold = 1 });

        // Trip the circuit
        Assert.Throws<InvalidOperationException>(() =>
            cb.Execute<int>(() => throw new InvalidOperationException("Test")));

        // Should reject with CircuitBreakerOpenException
        Assert.Throws<CircuitBreakerOpenException>(() =>
            cb.Execute(() => 42));
    }

    [Fact]
    public void CircuitBreaker_SuccessResetsFailureCount()
    {
        var config = new CircuitBreakerConfig { FailureThreshold = 3 };
        var cb = new CircuitBreaker(config);

        // 2 failures
        for (int i = 0; i < 2; i++)
        {
            try { cb.Execute<int>(() => throw new InvalidOperationException()); }
            catch { }
        }

        // 1 success - should reset
        cb.Execute(() => 42);

        // 2 more failures - should still be closed (reset occurred)
        for (int i = 0; i < 2; i++)
        {
            try { cb.Execute<int>(() => throw new InvalidOperationException()); }
            catch { }
        }

        Assert.Equal(CircuitBreakerState.Closed, cb.State);
    }

    [Fact]
    public void CircuitBreaker_ManualTrip()
    {
        var cb = new CircuitBreaker();
        cb.Trip();
        Assert.Equal(CircuitBreakerState.Open, cb.State);
    }

    [Fact]
    public void CircuitBreaker_ManualReset()
    {
        var cb = new CircuitBreaker(new CircuitBreakerConfig { FailureThreshold = 1 });

        // Trip the circuit
        try { cb.Execute<int>(() => throw new InvalidOperationException()); }
        catch { }

        Assert.Equal(CircuitBreakerState.Open, cb.State);

        // Reset
        cb.Reset();
        Assert.Equal(CircuitBreakerState.Closed, cb.State);
    }

    [Fact]
    public async Task CircuitBreaker_TransitionsToHalfOpenAfterDuration()
    {
        var config = new CircuitBreakerConfig
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(50)
        };
        var cb = new CircuitBreaker(config);

        // Trip the circuit
        try { cb.Execute<int>(() => throw new InvalidOperationException()); }
        catch { }

        Assert.Equal(CircuitBreakerState.Open, cb.State);

        // Wait for open duration
        await Task.Delay(100);

        // Should transition to half-open on next state check
        Assert.Equal(CircuitBreakerState.HalfOpen, cb.State);
    }

    [Fact]
    public void CircuitBreaker_OnlyHandlesConfiguredExceptions()
    {
        var config = new CircuitBreakerConfig
        {
            FailureThreshold = 1,
            ShouldHandle = ex => ex is IOException
        };
        var cb = new CircuitBreaker(config);

        // InvalidOperationException should not be handled
        Assert.Throws<InvalidOperationException>(() =>
            cb.Execute<int>(() => throw new InvalidOperationException()));

        // Circuit should still be closed
        Assert.Equal(CircuitBreakerState.Closed, cb.State);

        // IOException should be handled
        Assert.Throws<IOException>(() =>
            cb.Execute<int>(() => throw new IOException()));

        Assert.Equal(CircuitBreakerState.Open, cb.State);
    }

    #endregion

    #region Retry Policy Tests

    [Fact]
    public async Task RetryPolicy_RetriesOnFailure()
    {
        var attempts = 0;
        var policy = new RetryPolicy(new RetryPolicyConfig
        {
            MaxRetries = 3,
            InitialDelay = TimeSpan.FromMilliseconds(10),
            ShouldRetry = _ => true
        });

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await policy.ExecuteAsync(async _ =>
            {
                attempts++;
                await Task.Delay(1);
                throw new InvalidOperationException("Test");
            });
        });

        // Initial attempt + 3 retries = 4 total attempts
        Assert.Equal(4, attempts);
    }

    [Fact]
    public async Task RetryPolicy_SucceedsOnRetry()
    {
        var attempts = 0;
        var policy = new RetryPolicy(new RetryPolicyConfig
        {
            MaxRetries = 3,
            InitialDelay = TimeSpan.FromMilliseconds(10),
            ShouldRetry = _ => true
        });

        var result = await policy.ExecuteAsync(async _ =>
        {
            attempts++;
            if (attempts < 3)
            {
                await Task.Delay(1);
                throw new InvalidOperationException("Transient error");
            }
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task RetryPolicy_UsesExponentialBackoff()
    {
        var delays = new List<TimeSpan>();
        var policy = new RetryPolicy(new RetryPolicyConfig
        {
            MaxRetries = 3,
            InitialDelay = TimeSpan.FromMilliseconds(100),
            BackoffStrategy = BackoffStrategy.Exponential,
            ShouldRetry = _ => true,
            OnRetry = (_, _, delay) => delays.Add(delay)
        });

        try
        {
            await policy.ExecuteAsync<int>(async _ =>
            {
                await Task.Delay(1);
                throw new InvalidOperationException();
            });
        }
        catch { }

        Assert.Equal(3, delays.Count);
        Assert.True(delays[1] > delays[0]); // Each delay should be larger
        Assert.True(delays[2] > delays[1]);
    }

    [Fact]
    public void RetryPolicy_ForTransientErrors_CreatesCorrectConfig()
    {
        var policy = RetryPolicy.ForTransientErrors(5);
        Assert.Equal(5, policy.MaxRetries);
    }

    [Fact]
    public void RetryPolicy_ForBrokerConnection_CreatesCorrectConfig()
    {
        var policy = RetryPolicy.ForBrokerConnection(10);
        Assert.Equal(10, policy.MaxRetries);
    }

    [Fact]
    public async Task RetryPolicy_ProvidesRetryContext()
    {
        var contexts = new List<RetryContext>();
        var policy = new RetryPolicy(new RetryPolicyConfig
        {
            MaxRetries = 2,
            InitialDelay = TimeSpan.FromMilliseconds(10),
            ShouldRetry = _ => true
        });

        try
        {
            await policy.ExecuteAsync<int>(async (ctx, ct) =>
            {
                contexts.Add(ctx);
                await Task.Delay(1, ct);
                throw new InvalidOperationException();
            });
        }
        catch { }

        Assert.Equal(3, contexts.Count); // Initial + 2 retries
        Assert.Equal(0, contexts[0].AttemptNumber);
        Assert.Equal(1, contexts[1].AttemptNumber);
        Assert.Equal(2, contexts[2].AttemptNumber);
        Assert.Null(contexts[0].LastException);
        Assert.NotNull(contexts[1].LastException);
    }

    #endregion

    #region Bulkhead Policy Tests

    [Fact]
    public async Task BulkheadPolicy_LimitsConcurrency()
    {
        using var bulkhead = new BulkheadPolicy(2, 0); // 2 concurrent, no queue
        var running = 0;
        var maxConcurrent = 0;
        var rejectedCount = 0;
        // Use explicit synchronization instead of timing-based delays
        var slotsOccupied = new SemaphoreSlim(0);
        var canFinish = new ManualResetEventSlim(false);

        async Task OccupySlot()
        {
            await bulkhead.ExecuteAsync(ct =>
            {
                var current = Interlocked.Increment(ref running);
                int currentMax;
                do { currentMax = maxConcurrent; }
                while (current > currentMax &&
                       Interlocked.CompareExchange(ref maxConcurrent, current, currentMax) != currentMax);

                slotsOccupied.Release(); // Signal: we're inside the bulkhead
                canFinish.Wait(ct);      // Hold the slot until released
                Interlocked.Decrement(ref running);
                return Task.FromResult(true);
            });
        }

        async Task TryAndGetRejected()
        {
            try
            {
                await bulkhead.ExecuteAsync(async _ =>
                {
                    await Task.Delay(10);
                    return true;
                });
            }
            catch (BulkheadRejectedException)
            {
                Interlocked.Increment(ref rejectedCount);
            }
        }

        // Start 2 tasks that will occupy both slots
        var slot1 = Task.Run(OccupySlot);
        var slot2 = Task.Run(OccupySlot);

        // Wait until both slots are definitely occupied
        await slotsOccupied.WaitAsync();
        await slotsOccupied.WaitAsync();

        // Now try 2 more - should be rejected (no queue, both slots full)
        var reject1 = Task.Run(TryAndGetRejected);
        var reject2 = Task.Run(TryAndGetRejected);
        await Task.WhenAll(reject1, reject2);

        // Release the occupied slots
        canFinish.Set();
        await Task.WhenAll(slot1, slot2);

        Assert.True(maxConcurrent <= 2, $"Max concurrent was {maxConcurrent}");
        Assert.Equal(2, rejectedCount);
    }

    [Fact]
    public async Task BulkheadPolicy_RejectsWhenFull()
    {
        using var bulkhead = new BulkheadPolicy(1, 0); // 1 concurrent, no queue
        using var cts = new CancellationTokenSource();
        var taskStarted = new TaskCompletionSource<bool>();

        var longTask = bulkhead.ExecuteAsync(async ct =>
        {
            taskStarted.SetResult(true);
            try
            {
                await Task.Delay(5000, ct);
            }
            catch (OperationCanceledException) { }
            return true;
        }, cts.Token);

        // Wait for the first task to definitely start
        await taskStarted.Task;
        await Task.Delay(50);

        // This should be rejected immediately
        await Assert.ThrowsAsync<BulkheadRejectedException>(async () =>
        {
            await bulkhead.ExecuteAsync(async _ =>
            {
                await Task.Delay(100);
                return true;
            });
        });

        // Cancel the long task
        await cts.CancelAsync();
        try { await longTask; } catch { }
    }

    [Fact]
    public async Task BulkheadPolicy_QueuesWhenConfigured()
    {
        using var bulkhead = new BulkheadPolicy(1, 2); // 1 concurrent, 2 queued
        var completionOrder = new List<int>();
        var lockObj = new object();

        async Task DoWork(int id)
        {
            await bulkhead.ExecuteAsync(async _ =>
            {
                await Task.Delay(50);
                lock (lockObj)
                {
                    completionOrder.Add(id);
                }
                return true;
            });
        }

        // Start 3 tasks - 1 will run, 2 will queue
        var tasks = new[]
        {
            Task.Run(() => DoWork(1)),
            Task.Run(() => DoWork(2)),
            Task.Run(() => DoWork(3))
        };

        await Task.WhenAll(tasks);

        // All 3 should complete
        Assert.Equal(3, completionOrder.Count);
    }

    [Fact]
    public void BulkheadPolicy_TracksStatistics()
    {
        using var bulkhead = new BulkheadPolicy(5, 10);

        Assert.Equal(5, bulkhead.MaxConcurrency);
        Assert.Equal(10, bulkhead.MaxQueuedRequests);
        Assert.Equal(0, bulkhead.CurrentConcurrency);
        Assert.Equal(5, bulkhead.AvailableSlots);
    }

    [Fact]
    public async Task BulkheadPolicy_TryExecuteReturnsFalseWhenRejected()
    {
        using var bulkhead = new BulkheadPolicy(1, 0);
        using var cts = new CancellationTokenSource();
        var taskStarted = new TaskCompletionSource<bool>();

        var longTask = bulkhead.ExecuteAsync(async ct =>
        {
            taskStarted.SetResult(true);
            try
            {
                await Task.Delay(5000, ct);
            }
            catch (OperationCanceledException) { }
            return 42;
        }, cts.Token);

        // Wait for task to start
        await taskStarted.Task;
        await Task.Delay(50);

        var (success, _) = await bulkhead.TryExecuteAsync(async _ =>
        {
            await Task.Delay(10);
            return 99;
        });

        Assert.False(success);

        // Cleanup
        await cts.CancelAsync();
        try { await longTask; } catch { }
    }

    #endregion

    #region Resilience Pipeline Tests

    [Fact]
    public async Task ResiliencePipeline_CombinesPolicies()
    {
        using var pipeline = ResiliencePipeline.Create()
            .WithRetry(2)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

        var attempts = 0;
        var result = await pipeline.ExecuteAsync(async _ =>
        {
            attempts++;
            if (attempts < 2)
                throw new IOException("Transient");
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task ResiliencePipeline_TimeoutWorks()
    {
        using var pipeline = ResiliencePipeline.Create()
            .WithTimeout(TimeSpan.FromMilliseconds(50))
            .Build();

        await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            await pipeline.ExecuteAsync(async ct =>
            {
                await Task.Delay(5000, ct);
                return true;
            });
        });
    }

    [Fact]
    public void ResiliencePipeline_ForBrokerConnections_HasExpectedConfiguration()
    {
        using var pipeline = ResiliencePipeline.ForBrokerConnections();
        Assert.NotNull(pipeline);
    }

    [Fact]
    public void ResiliencePipeline_ForProduceOperations_HasExpectedConfiguration()
    {
        using var pipeline = ResiliencePipeline.ForProduceOperations();
        Assert.NotNull(pipeline);
    }

    [Fact]
    public void ResiliencePipeline_ForFetchOperations_HasExpectedConfiguration()
    {
        using var pipeline = ResiliencePipeline.ForFetchOperations();
        Assert.NotNull(pipeline);
    }

    [Fact]
    public async Task ResiliencePipeline_CircuitBreakerIntegration()
    {
        var stateChanges = new List<(CircuitBreakerState From, CircuitBreakerState To)>();
        var circuitBreaker = new CircuitBreaker(new CircuitBreakerConfig
        {
            FailureThreshold = 2,
            OpenDuration = TimeSpan.FromSeconds(5), // Long enough for assertion to check state
            OnStateChange = (from, to) => stateChanges.Add((from, to))
        });

        using var pipeline = ResiliencePipeline.Create()
            .WithCircuitBreaker(circuitBreaker)
            .Build();

        // Cause failures to trip the circuit
        for (int i = 0; i < 2; i++)
        {
            try
            {
                await pipeline.ExecuteAsync<int>(async _ =>
                {
                    await Task.Delay(1);
                    throw new IOException("Fail");
                });
            }
            catch (IOException) { }
        }

        // Circuit should be open now
        Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);

        // Next call should fail with CircuitBreakerOpenException
        await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
        {
            await pipeline.ExecuteAsync(async _ =>
            {
                await Task.Delay(1);
                return true;
            });
        });
    }

    #endregion

    #region Exception Tests

    [Fact]
    public void CircuitBreakerOpenException_HasRequiredConstructors()
    {
        var ex1 = new CircuitBreakerOpenException();
        Assert.Equal(TimeSpan.Zero, ex1.RetryAfter);

        var ex2 = new CircuitBreakerOpenException("Custom message");
        Assert.Equal("Custom message", ex2.Message);

        var inner = new InvalidOperationException();
        var ex3 = new CircuitBreakerOpenException("Message", inner);
        Assert.Same(inner, ex3.InnerException);

        var ex4 = new CircuitBreakerOpenException(TimeSpan.FromSeconds(30));
        Assert.Equal(TimeSpan.FromSeconds(30), ex4.RetryAfter);
    }

    [Fact]
    public void BulkheadRejectedException_HasRequiredConstructors()
    {
        var ex1 = new BulkheadRejectedException();
        Assert.NotNull(ex1.Message);

        var ex2 = new BulkheadRejectedException("Custom message");
        Assert.Equal("Custom message", ex2.Message);

        var inner = new InvalidOperationException();
        var ex3 = new BulkheadRejectedException("Message", inner);
        Assert.Same(inner, ex3.InnerException);

        var ex4 = new BulkheadRejectedException(5, 10);
        Assert.Equal(5, ex4.CurrentConcurrency);
        Assert.Equal(10, ex4.QueueLength);
    }

    #endregion
}

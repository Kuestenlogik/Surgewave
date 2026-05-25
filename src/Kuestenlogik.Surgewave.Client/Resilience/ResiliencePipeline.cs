namespace Kuestenlogik.Surgewave.Client.Resilience;

/// <summary>
/// A composable pipeline that combines multiple resilience policies.
///
/// The execution order is:
/// 1. Bulkhead (limits concurrency)
/// 2. Circuit Breaker (prevents cascading failures)
/// 3. Retry (handles transient failures)
/// 4. Timeout (prevents hanging operations)
/// 5. The actual operation
///
/// This order ensures:
/// - Concurrency is limited before any processing
/// - Circuit breaker can prevent retries on known-failing services
/// - Retries happen within the circuit breaker
/// - Timeouts apply to individual attempts
/// </summary>
public sealed class ResiliencePipeline : IDisposable
{
    private readonly BulkheadPolicy? _bulkhead;
    private readonly CircuitBreaker? _circuitBreaker;
    private readonly RetryPolicy? _retryPolicy;
    private readonly TimeSpan? _timeout;
    private bool _disposed;

    internal ResiliencePipeline(
        BulkheadPolicy? bulkhead,
        CircuitBreaker? circuitBreaker,
        RetryPolicy? retryPolicy,
        TimeSpan? timeout)
    {
        _bulkhead = bulkhead;
        _circuitBreaker = circuitBreaker;
        _retryPolicy = retryPolicy;
        _timeout = timeout;
    }

    /// <summary>
    /// Execute an operation through the resilience pipeline.
    /// </summary>
    public async Task<TResult> ExecuteAsync<TResult>(
        Func<CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Build the execution chain from inside out
        Func<CancellationToken, Task<TResult>> current = operation;

        // Apply timeout (innermost)
        if (_timeout.HasValue)
        {
            var timeoutValue = _timeout.Value;
            var inner = current;
            current = async ct =>
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeoutCts.CancelAfter(timeoutValue);
                try
                {
                    return await inner(timeoutCts.Token);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    throw new TimeoutException($"Operation timed out after {timeoutValue.TotalSeconds:F1} seconds");
                }
            };
        }

        // Apply retry policy
        if (_retryPolicy != null)
        {
            var retryPolicy = _retryPolicy;
            var inner = current;
            current = ct => retryPolicy.ExecuteAsync(inner, ct);
        }

        // Apply circuit breaker
        if (_circuitBreaker != null)
        {
            var circuitBreaker = _circuitBreaker;
            var inner = current;
            current = ct => circuitBreaker.ExecuteAsync(inner, ct);
        }

        // Apply bulkhead (outermost)
        if (_bulkhead != null)
        {
            return await _bulkhead.ExecuteAsync(current, cancellationToken);
        }

        return await current(cancellationToken);
    }

    /// <summary>
    /// Execute a void operation through the resilience pipeline.
    /// </summary>
    public async Task ExecuteAsync(
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken = default)
    {
        await ExecuteAsync(async ct =>
        {
            await operation(ct);
            return true;
        }, cancellationToken);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _bulkhead?.Dispose();
    }

    /// <summary>
    /// Create a new resilience pipeline builder.
    /// </summary>
    public static ResiliencePipelineBuilder Create() => new();

    /// <summary>
    /// Create a default pipeline for broker connections.
    /// Includes: Bulkhead (100 concurrent), Circuit Breaker, Retry (5 attempts), Timeout (30s)
    /// </summary>
    public static ResiliencePipeline ForBrokerConnections()
    {
        return Create()
            .WithBulkhead(100, 50)
            .WithCircuitBreaker(new CircuitBreakerConfig
            {
                FailureThreshold = 5,
                OpenDuration = TimeSpan.FromSeconds(30),
                SuccessThresholdInHalfOpen = 2
            })
            .WithRetry(new RetryPolicyConfig
            {
                MaxRetries = 5,
                InitialDelay = TimeSpan.FromMilliseconds(500),
                BackoffStrategy = BackoffStrategy.ExponentialWithJitter
            })
            .WithTimeout(TimeSpan.FromSeconds(30))
            .Build();
    }

    /// <summary>
    /// Create a default pipeline for produce operations.
    /// Includes: Circuit Breaker, Retry (3 attempts), Timeout (10s)
    /// </summary>
    public static ResiliencePipeline ForProduceOperations()
    {
        return Create()
            .WithCircuitBreaker(new CircuitBreakerConfig
            {
                FailureThreshold = 10,
                OpenDuration = TimeSpan.FromSeconds(15),
                SuccessThresholdInHalfOpen = 1
            })
            .WithRetry(new RetryPolicyConfig
            {
                MaxRetries = 3,
                InitialDelay = TimeSpan.FromMilliseconds(100),
                BackoffStrategy = BackoffStrategy.ExponentialWithJitter
            })
            .WithTimeout(TimeSpan.FromSeconds(10))
            .Build();
    }

    /// <summary>
    /// Create a default pipeline for fetch operations.
    /// Includes: Bulkhead, Circuit Breaker, Retry (3 attempts), Timeout (30s)
    /// </summary>
    public static ResiliencePipeline ForFetchOperations()
    {
        return Create()
            .WithBulkhead(50, 100)
            .WithCircuitBreaker(new CircuitBreakerConfig
            {
                FailureThreshold = 5,
                OpenDuration = TimeSpan.FromSeconds(20),
                SuccessThresholdInHalfOpen = 1
            })
            .WithRetry(new RetryPolicyConfig
            {
                MaxRetries = 3,
                InitialDelay = TimeSpan.FromMilliseconds(200),
                BackoffStrategy = BackoffStrategy.ExponentialWithJitter
            })
            .WithTimeout(TimeSpan.FromSeconds(30))
            .Build();
    }
}

/// <summary>
/// Builder for creating resilience pipelines.
/// Note: The builder transfers ownership of BulkheadPolicy to the built pipeline,
/// so the pipeline is responsible for disposing it.
/// </summary>
public sealed class ResiliencePipelineBuilder
{
    private BulkheadPolicy? _bulkhead;
    private CircuitBreaker? _circuitBreaker;
    private RetryPolicy? _retryPolicy;
    private TimeSpan? _timeout;

    /// <summary>
    /// Add a bulkhead policy to limit concurrency.
    /// </summary>
    public ResiliencePipelineBuilder WithBulkhead(int maxConcurrency, int maxQueued = 0)
    {
        _bulkhead = new BulkheadPolicy(maxConcurrency, maxQueued);
        return this;
    }

    /// <summary>
    /// Add a bulkhead policy with configuration.
    /// </summary>
    public ResiliencePipelineBuilder WithBulkhead(BulkheadPolicyConfig config)
    {
        _bulkhead = new BulkheadPolicy(config);
        return this;
    }

    /// <summary>
    /// Add a circuit breaker with default configuration.
    /// </summary>
    public ResiliencePipelineBuilder WithCircuitBreaker()
    {
        _circuitBreaker = new CircuitBreaker();
        return this;
    }

    /// <summary>
    /// Add a circuit breaker with configuration.
    /// </summary>
    public ResiliencePipelineBuilder WithCircuitBreaker(CircuitBreakerConfig config)
    {
        _circuitBreaker = new CircuitBreaker(config);
        return this;
    }

    /// <summary>
    /// Add an existing circuit breaker (allows sharing across pipelines).
    /// </summary>
    public ResiliencePipelineBuilder WithCircuitBreaker(CircuitBreaker circuitBreaker)
    {
        _circuitBreaker = circuitBreaker;
        return this;
    }

    /// <summary>
    /// Add a retry policy with default configuration.
    /// </summary>
    public ResiliencePipelineBuilder WithRetry(int maxRetries = 3)
    {
        _retryPolicy = new RetryPolicy(new RetryPolicyConfig { MaxRetries = maxRetries });
        return this;
    }

    /// <summary>
    /// Add a retry policy with configuration.
    /// </summary>
    public ResiliencePipelineBuilder WithRetry(RetryPolicyConfig config)
    {
        _retryPolicy = new RetryPolicy(config);
        return this;
    }

    /// <summary>
    /// Add a timeout to operations.
    /// </summary>
    public ResiliencePipelineBuilder WithTimeout(TimeSpan timeout)
    {
        _timeout = timeout;
        return this;
    }

    /// <summary>
    /// Build the resilience pipeline.
    /// Ownership of the bulkhead is transferred to the pipeline.
    /// </summary>
    public ResiliencePipeline Build()
    {
        var bulkhead = _bulkhead;
        _bulkhead = null; // Transfer ownership to pipeline
        return new ResiliencePipeline(bulkhead, _circuitBreaker, _retryPolicy, _timeout);
    }
}

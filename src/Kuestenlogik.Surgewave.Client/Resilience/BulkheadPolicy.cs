namespace Kuestenlogik.Surgewave.Client.Resilience;

/// <summary>
/// Configuration for the bulkhead policy.
/// </summary>
public sealed class BulkheadPolicyConfig
{
    /// <summary>
    /// Maximum number of concurrent executions allowed.
    /// Default: 10
    /// </summary>
    public int MaxConcurrency { get; set; } = 10;

    /// <summary>
    /// Maximum number of requests that can wait in the queue.
    /// Set to 0 to disable queuing (reject immediately when at capacity).
    /// Default: 20
    /// </summary>
    public int MaxQueuedRequests { get; set; } = 20;

    /// <summary>
    /// Maximum time a request can wait in the queue.
    /// Default: 30 seconds
    /// </summary>
    public TimeSpan QueueTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Callback when a request is rejected due to bulkhead limits.
    /// </summary>
    public Action? OnRejected { get; set; }

    /// <summary>
    /// Callback when a request enters the queue.
    /// </summary>
    public Action<int>? OnQueued { get; set; }
}

/// <summary>
/// Exception thrown when the bulkhead rejects a request.
/// </summary>
public sealed class BulkheadRejectedException : Exception
{
    /// <summary>
    /// Current number of concurrent executions.
    /// </summary>
    public int CurrentConcurrency { get; }

    /// <summary>
    /// Current queue length.
    /// </summary>
    public int QueueLength { get; }

    public BulkheadRejectedException() : base("Bulkhead capacity exceeded.")
    {
    }

    public BulkheadRejectedException(string message) : base(message)
    {
    }

    public BulkheadRejectedException(string message, Exception innerException) : base(message, innerException)
    {
    }

    public BulkheadRejectedException(int currentConcurrency, int queueLength)
        : base($"Bulkhead capacity exceeded. Concurrent: {currentConcurrency}, Queued: {queueLength}")
    {
        CurrentConcurrency = currentConcurrency;
        QueueLength = queueLength;
    }
}

/// <summary>
/// Bulkhead policy implementation for isolation and load shedding.
///
/// The bulkhead pattern limits the number of concurrent operations to prevent
/// resource exhaustion and provide isolation between different parts of a system.
///
/// When the concurrency limit is reached, requests can optionally queue up to
/// a maximum queue size, or be rejected immediately.
/// </summary>
public sealed class BulkheadPolicy : IDisposable
{
    private readonly BulkheadPolicyConfig _config;
    private readonly SemaphoreSlim _executionSemaphore;
    private readonly SemaphoreSlim _queueSemaphore;
    private int _currentConcurrency;
    private int _currentQueueLength;
    private bool _disposed;

    /// <summary>
    /// Current number of concurrent executions.
    /// </summary>
    public int CurrentConcurrency => _currentConcurrency;

    /// <summary>
    /// Current number of requests waiting in the queue.
    /// </summary>
    public int QueueLength => _currentQueueLength;

    /// <summary>
    /// Maximum concurrency limit.
    /// </summary>
    public int MaxConcurrency => _config.MaxConcurrency;

    /// <summary>
    /// Maximum queue size.
    /// </summary>
    public int MaxQueuedRequests => _config.MaxQueuedRequests;

    /// <summary>
    /// Available execution slots.
    /// </summary>
    public int AvailableSlots => _config.MaxConcurrency - _currentConcurrency;

    /// <summary>
    /// Creates a bulkhead policy with default configuration.
    /// </summary>
    public BulkheadPolicy() : this(new BulkheadPolicyConfig())
    {
    }

    /// <summary>
    /// Creates a bulkhead policy with the specified configuration.
    /// </summary>
    public BulkheadPolicy(BulkheadPolicyConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _executionSemaphore = new SemaphoreSlim(_config.MaxConcurrency, _config.MaxConcurrency);
        // Only create queue semaphore if queuing is enabled
        _queueSemaphore = _config.MaxQueuedRequests > 0
            ? new SemaphoreSlim(_config.MaxQueuedRequests, _config.MaxQueuedRequests)
            : new SemaphoreSlim(0, 1); // Dummy semaphore that's always full
    }

    /// <summary>
    /// Creates a bulkhead with the specified concurrency and queue limits.
    /// </summary>
    public BulkheadPolicy(int maxConcurrency, int maxQueuedRequests = 0)
        : this(new BulkheadPolicyConfig
        {
            MaxConcurrency = maxConcurrency,
            MaxQueuedRequests = maxQueuedRequests
        })
    {
    }

    /// <summary>
    /// Execute an operation within the bulkhead limits.
    /// </summary>
    public async Task<TResult> ExecuteAsync<TResult>(
        Func<CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Try to acquire an execution slot directly
        if (_executionSemaphore.Wait(0))
        {
            Interlocked.Increment(ref _currentConcurrency);
            try
            {
                return await operation(cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref _currentConcurrency);
                _executionSemaphore.Release();
            }
        }

        // No immediate slot available - try to queue
        if (_config.MaxQueuedRequests <= 0 || !_queueSemaphore.Wait(0))
        {
            // Cannot queue - reject immediately
            _config.OnRejected?.Invoke();
            throw new BulkheadRejectedException(_currentConcurrency, _currentQueueLength);
        }

        // We're in the queue
        Interlocked.Increment(ref _currentQueueLength);
        _config.OnQueued?.Invoke(_currentQueueLength);

        try
        {
            // Wait for an execution slot with timeout
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(_config.QueueTimeout);

            try
            {
                await _executionSemaphore.WaitAsync(timeoutCts.Token);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // Queue timeout - reject
                _config.OnRejected?.Invoke();
                throw new BulkheadRejectedException(_currentConcurrency, _currentQueueLength);
            }

            Interlocked.Decrement(ref _currentQueueLength);
            _queueSemaphore.Release();

            Interlocked.Increment(ref _currentConcurrency);
            try
            {
                return await operation(cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref _currentConcurrency);
                _executionSemaphore.Release();
            }
        }
        catch
        {
            // Make sure we clean up queue state on any error
            if (_currentQueueLength > 0)
            {
                Interlocked.Decrement(ref _currentQueueLength);
                try { _queueSemaphore.Release(); } catch { /* ignore */ }
            }
            throw;
        }
    }

    /// <summary>
    /// Execute a void operation within the bulkhead limits.
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

    /// <summary>
    /// Execute a synchronous operation within the bulkhead limits.
    /// </summary>
    public TResult Execute<TResult>(Func<TResult> operation)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Try to acquire an execution slot directly
        if (_executionSemaphore.Wait(0))
        {
            Interlocked.Increment(ref _currentConcurrency);
            try
            {
                return operation();
            }
            finally
            {
                Interlocked.Decrement(ref _currentConcurrency);
                _executionSemaphore.Release();
            }
        }

        // No immediate slot available - try to queue
        if (_config.MaxQueuedRequests <= 0 || !_queueSemaphore.Wait(0))
        {
            _config.OnRejected?.Invoke();
            throw new BulkheadRejectedException(_currentConcurrency, _currentQueueLength);
        }

        Interlocked.Increment(ref _currentQueueLength);
        _config.OnQueued?.Invoke(_currentQueueLength);

        try
        {
            // Wait for an execution slot with timeout
            if (!_executionSemaphore.Wait(_config.QueueTimeout))
            {
                _config.OnRejected?.Invoke();
                throw new BulkheadRejectedException(_currentConcurrency, _currentQueueLength);
            }

            Interlocked.Decrement(ref _currentQueueLength);
            _queueSemaphore.Release();

            Interlocked.Increment(ref _currentConcurrency);
            try
            {
                return operation();
            }
            finally
            {
                Interlocked.Decrement(ref _currentConcurrency);
                _executionSemaphore.Release();
            }
        }
        catch
        {
            if (_currentQueueLength > 0)
            {
                Interlocked.Decrement(ref _currentQueueLength);
                try { _queueSemaphore.Release(); } catch { /* ignore */ }
            }
            throw;
        }
    }

    /// <summary>
    /// Try to execute an operation, returning false if rejected.
    /// </summary>
    public async Task<(bool Success, TResult? Result)> TryExecuteAsync<TResult>(
        Func<CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await ExecuteAsync(operation, cancellationToken);
            return (true, result);
        }
        catch (BulkheadRejectedException)
        {
            return (false, default);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _executionSemaphore.Dispose();
        _queueSemaphore.Dispose();
    }
}

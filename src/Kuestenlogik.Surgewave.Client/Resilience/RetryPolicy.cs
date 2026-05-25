namespace Kuestenlogik.Surgewave.Client.Resilience;

/// <summary>
/// Backoff strategy for retry delays.
/// </summary>
public enum BackoffStrategy
{
    /// <summary>
    /// Fixed delay between retries.
    /// </summary>
    Fixed,

    /// <summary>
    /// Exponential backoff (delay doubles each retry).
    /// </summary>
    Exponential,

    /// <summary>
    /// Exponential backoff with random jitter to prevent thundering herd.
    /// </summary>
    ExponentialWithJitter,

    /// <summary>
    /// Linear backoff (delay increases linearly).
    /// </summary>
    Linear
}

/// <summary>
/// Configuration for the retry policy.
/// </summary>
public sealed class RetryPolicyConfig
{
    /// <summary>
    /// Maximum number of retry attempts. Default: 3
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Initial delay between retries. Default: 100ms
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Maximum delay between retries. Default: 30 seconds
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Backoff strategy for calculating delays. Default: ExponentialWithJitter
    /// </summary>
    public BackoffStrategy BackoffStrategy { get; set; } = BackoffStrategy.ExponentialWithJitter;

    /// <summary>
    /// Multiplier for exponential backoff. Default: 2.0
    /// </summary>
    public double BackoffMultiplier { get; set; } = 2.0;

    /// <summary>
    /// Predicate to determine which exceptions should trigger a retry.
    /// If null, all exceptions trigger retries.
    /// </summary>
    public Func<Exception, bool>? ShouldRetry { get; set; }

    /// <summary>
    /// Predicate to determine which results should trigger a retry.
    /// </summary>
    public Func<object?, bool>? ShouldRetryResult { get; set; }

    /// <summary>
    /// Callback invoked before each retry attempt.
    /// Parameters: exception (or null), attempt number, delay before retry
    /// </summary>
    public Action<Exception?, int, TimeSpan>? OnRetry { get; set; }
}

/// <summary>
/// Context information passed during retry operations.
/// </summary>
public sealed class RetryContext
{
    /// <summary>
    /// Current retry attempt (0 for first attempt, 1 for first retry, etc.)
    /// </summary>
    public int AttemptNumber { get; init; }

    /// <summary>
    /// Total elapsed time since the first attempt.
    /// </summary>
    public TimeSpan TotalElapsed { get; init; }

    /// <summary>
    /// The exception that triggered this retry (null on first attempt).
    /// </summary>
    public Exception? LastException { get; init; }

    /// <summary>
    /// Remaining retry attempts.
    /// </summary>
    public int RemainingRetries { get; init; }
}

/// <summary>
/// Retry policy implementation for transient fault handling.
/// Supports various backoff strategies and configurable retry conditions.
/// </summary>
public sealed class RetryPolicy
{
    private readonly RetryPolicyConfig _config;
    private static readonly Random SharedRandom = Random.Shared;

    /// <summary>
    /// Creates a retry policy with default configuration.
    /// </summary>
    public RetryPolicy() : this(new RetryPolicyConfig())
    {
    }

    /// <summary>
    /// Creates a retry policy with the specified configuration.
    /// </summary>
    public RetryPolicy(RetryPolicyConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <summary>
    /// Maximum number of retry attempts configured.
    /// </summary>
    public int MaxRetries => _config.MaxRetries;

    /// <summary>
    /// Execute an operation with retry logic.
    /// </summary>
    public async Task<TResult> ExecuteAsync<TResult>(
        Func<RetryContext, CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        Exception? lastException = null;

        for (int attempt = 0; attempt <= _config.MaxRetries; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var context = new RetryContext
            {
                AttemptNumber = attempt,
                TotalElapsed = DateTime.UtcNow - startTime,
                LastException = lastException,
                RemainingRetries = _config.MaxRetries - attempt
            };

            try
            {
                var result = await operation(context, cancellationToken);

                // Check if we should retry based on result
                if (_config.ShouldRetryResult != null && _config.ShouldRetryResult(result))
                {
                    if (attempt < _config.MaxRetries)
                    {
                        var delay = CalculateDelay(attempt);
                        _config.OnRetry?.Invoke(null, attempt + 1, delay);
                        await Task.Delay(delay, cancellationToken);
                        continue;
                    }
                }

                return result;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                lastException = ex;

                // Check if we should retry this exception
                if (!ShouldRetry(ex) || attempt >= _config.MaxRetries)
                {
                    throw;
                }

                var delay = CalculateDelay(attempt);
                _config.OnRetry?.Invoke(ex, attempt + 1, delay);
                await Task.Delay(delay, cancellationToken);
            }
        }

        // This should never be reached, but just in case
        throw lastException ?? new InvalidOperationException("Retry logic error");
    }

    /// <summary>
    /// Execute an operation with retry logic (simplified overload without context).
    /// </summary>
    public Task<TResult> ExecuteAsync<TResult>(
        Func<CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default)
    {
        return ExecuteAsync((_, ct) => operation(ct), cancellationToken);
    }

    /// <summary>
    /// Execute a void operation with retry logic.
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
    /// Execute a synchronous operation with retry logic.
    /// </summary>
    public TResult Execute<TResult>(Func<TResult> operation)
    {
        Exception? lastException = null;

        for (int attempt = 0; attempt <= _config.MaxRetries; attempt++)
        {
            try
            {
                var result = operation();

                if (_config.ShouldRetryResult != null && _config.ShouldRetryResult(result))
                {
                    if (attempt < _config.MaxRetries)
                    {
                        var delay = CalculateDelay(attempt);
                        _config.OnRetry?.Invoke(null, attempt + 1, delay);
                        Thread.Sleep(delay);
                        continue;
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                lastException = ex;

                if (!ShouldRetry(ex) || attempt >= _config.MaxRetries)
                {
                    throw;
                }

                var delay = CalculateDelay(attempt);
                _config.OnRetry?.Invoke(ex, attempt + 1, delay);
                Thread.Sleep(delay);
            }
        }

        throw lastException ?? new InvalidOperationException("Retry logic error");
    }

    private bool ShouldRetry(Exception ex)
    {
        if (_config.ShouldRetry == null)
        {
            // Default: retry on transient errors
            return ex is IOException
                or TimeoutException
                or HttpRequestException
                || (ex is InvalidOperationException ioe && ioe.Message.Contains("transient", StringComparison.OrdinalIgnoreCase));
        }

        return _config.ShouldRetry(ex);
    }

    private TimeSpan CalculateDelay(int attempt)
    {
        var delay = _config.BackoffStrategy switch
        {
            BackoffStrategy.Fixed => _config.InitialDelay,
            BackoffStrategy.Exponential => TimeSpan.FromMilliseconds(
                _config.InitialDelay.TotalMilliseconds * Math.Pow(_config.BackoffMultiplier, attempt)),
            BackoffStrategy.ExponentialWithJitter => CalculateExponentialWithJitter(attempt),
            BackoffStrategy.Linear => TimeSpan.FromMilliseconds(
                _config.InitialDelay.TotalMilliseconds * (attempt + 1)),
            _ => _config.InitialDelay
        };

        // Cap at max delay
        return delay > _config.MaxDelay ? _config.MaxDelay : delay;
    }

    private TimeSpan CalculateExponentialWithJitter(int attempt)
    {
        var baseDelay = _config.InitialDelay.TotalMilliseconds * Math.Pow(_config.BackoffMultiplier, attempt);

        // Add random jitter between 0% and 25% of the base delay
        // Using Random.Shared for jitter - cryptographic security not needed for backoff timing
        var jitter = baseDelay * SharedRandom.NextDouble() * 0.25;
        var delayWithJitter = baseDelay + jitter;

        return TimeSpan.FromMilliseconds(delayWithJitter);
    }

    /// <summary>
    /// Create a retry policy for transient network errors.
    /// </summary>
    public static RetryPolicy ForTransientErrors(int maxRetries = 3)
    {
        return new RetryPolicy(new RetryPolicyConfig
        {
            MaxRetries = maxRetries,
            InitialDelay = TimeSpan.FromMilliseconds(100),
            BackoffStrategy = BackoffStrategy.ExponentialWithJitter,
            ShouldRetry = ex => ex is IOException
                or TimeoutException
                or HttpRequestException
                or OperationCanceledException { InnerException: TimeoutException }
        });
    }

    /// <summary>
    /// Create a retry policy for broker connection errors.
    /// </summary>
    public static RetryPolicy ForBrokerConnection(int maxRetries = 5)
    {
        return new RetryPolicy(new RetryPolicyConfig
        {
            MaxRetries = maxRetries,
            InitialDelay = TimeSpan.FromMilliseconds(500),
            MaxDelay = TimeSpan.FromSeconds(30),
            BackoffStrategy = BackoffStrategy.ExponentialWithJitter,
            ShouldRetry = ex => ex is IOException
                or TimeoutException
                or System.Net.Sockets.SocketException
        });
    }
}

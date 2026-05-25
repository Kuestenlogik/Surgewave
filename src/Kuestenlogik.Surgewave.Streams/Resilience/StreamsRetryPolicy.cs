namespace Kuestenlogik.Surgewave.Streams.Resilience;

/// <summary>
/// Lightweight retry policy for streams processing.
/// Supports fixed, exponential, exponential-with-jitter, and linear backoff.
/// Optionally integrates with a <see cref="CircuitBreaker"/>.
/// </summary>
public sealed class StreamsRetryPolicy
{
    private readonly StreamsRetryConfig _config;
    private readonly CircuitBreaker? _circuitBreaker;
    private static readonly System.Security.Cryptography.RandomNumberGenerator CryptoRng =
        System.Security.Cryptography.RandomNumberGenerator.Create();

    public StreamsRetryPolicy(StreamsRetryConfig config)
    {
        _config = config;

        if (config.EnableCircuitBreaker)
            _circuitBreaker = new CircuitBreaker(
                config.CircuitBreakerThreshold,
                config.CircuitBreakerResetTimeout);
    }

    /// <summary>
    /// Executes an action with retry logic, recording metrics.
    /// </summary>
    public void Execute(Action action, StreamsMetrics? metrics = null)
    {
        var attempt = 0;

        while (true)
        {
            try
            {
                action();
                _circuitBreaker?.RecordSuccess();
                return;
            }
            catch (Exception ex) when (attempt < _config.MaxRetries && ShouldRetry(ex))
            {
                _circuitBreaker?.RecordFailure();
                attempt++;
                metrics?.RecordRetry();

                var delay = CalculateDelay(attempt);
                Thread.Sleep(delay);
            }
            catch (Exception) when (attempt >= _config.MaxRetries)
            {
                _circuitBreaker?.RecordFailure();
                metrics?.RecordRetryExhausted();
                throw;
            }
        }
    }

    /// <summary>
    /// Executes a function with retry logic, returning the result.
    /// </summary>
    public T Execute<T>(Func<T> action, StreamsMetrics? metrics = null)
    {
        var attempt = 0;

        while (true)
        {
            try
            {
                var result = action();
                _circuitBreaker?.RecordSuccess();
                return result;
            }
            catch (Exception ex) when (attempt < _config.MaxRetries && ShouldRetry(ex))
            {
                _circuitBreaker?.RecordFailure();
                attempt++;
                metrics?.RecordRetry();

                var delay = CalculateDelay(attempt);
                Thread.Sleep(delay);
            }
            catch (Exception) when (attempt >= _config.MaxRetries)
            {
                _circuitBreaker?.RecordFailure();
                metrics?.RecordRetryExhausted();
                throw;
            }
        }
    }

    /// <summary>
    /// Asynchronously executes an action with retry logic.
    /// Uses <c>Task.Delay</c> instead of <c>Thread.Sleep</c>.
    /// Throws <see cref="RetryExhaustedException"/> when all attempts are consumed.
    /// Optionally checks a <see cref="CircuitBreaker"/> before each attempt.
    /// </summary>
    public async Task ExecuteAsync(
        Func<Task> action,
        StreamsMetrics? metrics = null,
        CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        var started = Environment.TickCount64;
        Exception? lastException = null;

        while (attempt <= _config.MaxRetries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_circuitBreaker is not null && !_circuitBreaker.AllowRequest())
                throw new InvalidOperationException(
                    $"Circuit breaker is {_circuitBreaker.State}; request rejected.");

            try
            {
                await action().ConfigureAwait(false);
                _circuitBreaker?.RecordSuccess();
                return;
            }
            catch (Exception ex) when (ShouldRetry(ex) && attempt < _config.MaxRetries)
            {
                lastException = ex;
                _circuitBreaker?.RecordFailure();
                attempt++;
                metrics?.RecordRetry();

                var delay = CalculateDelay(attempt);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                lastException = ex;
                _circuitBreaker?.RecordFailure();
                metrics?.RecordRetryExhausted();
                break;
            }
        }

        var elapsed = TimeSpan.FromMilliseconds(Environment.TickCount64 - started);
        throw new RetryExhaustedException(attempt + 1, elapsed, lastException!);
    }

    private bool ShouldRetry(Exception ex)
    {
        if (_config.ShouldRetry != null)
            return _config.ShouldRetry(ex);

        // Default: retry transient exceptions, not argument/logic errors
        return ex is not (ArgumentException or InvalidOperationException or NotSupportedException);
    }

    public TimeSpan CalculateDelay(int attempt)
    {
        var delay = _config.BackoffStrategy switch
        {
            BackoffStrategy.Fixed => _config.InitialDelay,
            BackoffStrategy.Exponential => TimeSpan.FromMilliseconds(
                _config.InitialDelay.TotalMilliseconds * Math.Pow(2, attempt - 1)),
            BackoffStrategy.ExponentialWithJitter => TimeSpan.FromMilliseconds(
                _config.InitialDelay.TotalMilliseconds * Math.Pow(2, attempt - 1) *
                (0.5 + GetCryptoDouble())),
            BackoffStrategy.Linear => TimeSpan.FromMilliseconds(
                _config.InitialDelay.TotalMilliseconds * attempt),
            _ => _config.InitialDelay
        };

        return delay > _config.MaxDelay ? _config.MaxDelay : delay;
    }

    private static double GetCryptoDouble()
    {
        Span<byte> bytes = stackalloc byte[8];
        CryptoRng.GetBytes(bytes);
        return (double)(BitConverter.ToUInt64(bytes) >> 11) / (1UL << 53);
    }
}

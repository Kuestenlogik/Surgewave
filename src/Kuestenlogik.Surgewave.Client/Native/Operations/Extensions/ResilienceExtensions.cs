using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Client.Native.Operations.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Extensions;

/// <summary>
/// Configuration for resilience policies.
/// </summary>
public sealed class ResilienceConfig
{
    public TimeSpan? Timeout { get; set; }
    public int RetryAttempts { get; set; }
    public TimeSpan RetryBackoff { get; set; } = TimeSpan.FromMilliseconds(100);
    public bool UseExponentialBackoff { get; set; } = true;
}

/// <summary>
/// Wrapper builder that adds resilience (timeout/retry) to any operation.
/// </summary>
/// <typeparam name="TResult">The result type of the operation.</typeparam>
public sealed class ResilientBuilder<TResult>
{
    private readonly Func<CancellationToken, Task<TResult>> _operation;
    private TimeSpan? _timeout;
    private int _retryAttempts;
    private TimeSpan _retryBackoff = TimeSpan.FromMilliseconds(100);
    private bool _exponentialBackoff = true;
    private Func<Exception, bool>? _retryPredicate;

    internal ResilientBuilder(Func<CancellationToken, Task<TResult>> operation)
    {
        _operation = operation;
    }

    /// <summary>
    /// Set operation timeout.
    /// </summary>
    public ResilientBuilder<TResult> WithTimeout(TimeSpan timeout)
    {
        _timeout = timeout;
        return this;
    }

    /// <summary>
    /// Configure retry policy.
    /// </summary>
    public ResilientBuilder<TResult> WithRetry(int attempts, TimeSpan? backoff = null)
    {
        _retryAttempts = attempts;
        if (backoff.HasValue)
            _retryBackoff = backoff.Value;
        return this;
    }

    /// <summary>
    /// Use exponential backoff for retries.
    /// </summary>
    public ResilientBuilder<TResult> WithExponentialBackoff(bool enabled = true)
    {
        _exponentialBackoff = enabled;
        return this;
    }

    /// <summary>
    /// Set a predicate to determine which exceptions should trigger retry.
    /// </summary>
    public ResilientBuilder<TResult> RetryWhen(Func<Exception, bool> predicate)
    {
        _retryPredicate = predicate;
        return this;
    }

    /// <summary>
    /// Execute the operation with configured resilience policies.
    /// </summary>
    public async Task<TResult> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        Exception? lastException = null;

        while (true)
        {
            try
            {
                return await ExecuteWithTimeoutAsync(cancellationToken);
            }
            catch (Exception ex) when (ShouldRetry(ex, attempt))
            {
                lastException = ex;
                attempt++;

                var delay = _exponentialBackoff
                    ? TimeSpan.FromMilliseconds(_retryBackoff.TotalMilliseconds * Math.Pow(2, attempt - 1))
                    : _retryBackoff;

                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    private async Task<TResult> ExecuteWithTimeoutAsync(CancellationToken cancellationToken)
    {
        if (_timeout == null)
            return await _operation(cancellationToken);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_timeout.Value);

        try
        {
            return await _operation(cts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Operation timed out after {_timeout.Value.TotalSeconds:F1} seconds");
        }
    }

    private bool ShouldRetry(Exception ex, int currentAttempt)
    {
        if (currentAttempt >= _retryAttempts)
            return false;

        if (_retryPredicate != null)
            return _retryPredicate(ex);

        // Default: retry on transient errors
        return ex is IOException or TimeoutException or InvalidOperationException;
    }
}

/// <summary>
/// Extension methods to add resilience to builders.
/// </summary>
public static class ResilienceExtensions
{
    /// <summary>
    /// Add resilience (timeout/retry) to a send operation.
    /// </summary>
    public static ResilientBuilder<long> WithResilience(this SendBuilder builder)
        => new(ct => builder.ExecuteAsync(ct));

    /// <summary>
    /// Add resilience (timeout/retry) to a batch send operation.
    /// </summary>
    public static ResilientBuilder<long> WithBatchResilience(this SendBuilder builder)
        => new(ct => builder.SendAllAsync(ct));

    /// <summary>
    /// Add resilience (timeout/retry) to a receive operation.
    /// </summary>
    public static ResilientBuilder<ReceiveResult> WithResilience(this ReceiveBuilder builder)
        => new(ct => builder.ExecuteAsync(ct));

    /// <summary>
    /// Add resilience (timeout/retry) to a typed send operation.
    /// </summary>
    public static ResilientBuilder<long> WithResilience<TKey, TValue>(this TypedSendBuilder<TKey, TValue> builder)
        => new(ct => builder.ExecuteAsync(ct));

    /// <summary>
    /// Add resilience (timeout/retry) to a typed batch send operation.
    /// </summary>
    public static ResilientBuilder<long> WithBatchResilience<TKey, TValue>(this TypedSendBuilder<TKey, TValue> builder)
        => new(ct => builder.SendAllAsync(ct));

    /// <summary>
    /// Add resilience (timeout/retry) to a topic create operation.
    /// </summary>
    public static ResilientBuilder<bool> WithResilience(this TopicCreateBuilder builder)
        => new(async ct =>
        {
            await builder.ExecuteAsync(ct);
            return true;
        });

    /// <summary>
    /// Add resilience (timeout/retry) to a schema registration operation.
    /// </summary>
    public static ResilientBuilder<SchemaRegistrationResult> WithResilience(this SchemaBuilder builder)
        => new(ct => builder.ExecuteAsync(ct));
}

namespace Kuestenlogik.Surgewave.Clustering;

/// <summary>
/// Provides exponential backoff retry logic for RPC calls.
/// </summary>
public static class RetryHelper
{
    /// <summary>
    /// Default initial delay for backoff (50ms).
    /// </summary>
    public const int DefaultInitialDelayMs = 50;

    /// <summary>
    /// Default maximum number of retries.
    /// </summary>
    public const int DefaultMaxRetries = 3;

    /// <summary>
    /// Executes an async operation with exponential backoff retry.
    /// </summary>
    /// <typeparam name="T">Result type</typeparam>
    /// <param name="operation">The async operation to execute</param>
    /// <param name="maxRetries">Maximum retry attempts (default: 3)</param>
    /// <param name="initialDelayMs">Initial delay in milliseconds (default: 50)</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Result of the operation, or default(T) if all retries failed</returns>
    public static async Task<T?> ExecuteWithBackoffAsync<T>(
        Func<Task<T>> operation,
        int maxRetries = DefaultMaxRetries,
        int initialDelayMs = DefaultInitialDelayMs,
        CancellationToken ct = default)
    {
        var delayMs = initialDelayMs;

        for (var attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw; // Don't retry on explicit cancellation
            }
            catch
            {
                if (attempt == maxRetries)
                    throw;

                await Task.Delay(delayMs, ct);
                delayMs *= 2; // Exponential backoff
            }
        }

        return default;
    }

    /// <summary>
    /// Executes an async operation with exponential backoff retry, returning success status.
    /// </summary>
    /// <typeparam name="T">Result type</typeparam>
    /// <param name="operation">The async operation to execute</param>
    /// <param name="onSuccess">Callback when operation succeeds</param>
    /// <param name="onRetry">Optional callback before each retry (attempt, exception)</param>
    /// <param name="maxRetries">Maximum retry attempts (default: 3)</param>
    /// <param name="initialDelayMs">Initial delay in milliseconds (default: 50)</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>True if successful, false if all retries failed</returns>
    public static async Task<bool> TryExecuteWithBackoffAsync<T>(
        Func<Task<T>> operation,
        Action<T> onSuccess,
        Action<int, Exception>? onRetry = null,
        int maxRetries = DefaultMaxRetries,
        int initialDelayMs = DefaultInitialDelayMs,
        CancellationToken ct = default)
    {
        var delayMs = initialDelayMs;

        for (var attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var result = await operation();
                onSuccess(result);
                return true;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                if (attempt < maxRetries)
                {
                    onRetry?.Invoke(attempt, ex);
                    await Task.Delay(delayMs, ct);
                    delayMs *= 2;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Calculates the delay for a given retry attempt using exponential backoff.
    /// </summary>
    /// <param name="attempt">Current attempt (1-based)</param>
    /// <param name="initialDelayMs">Initial delay in milliseconds</param>
    /// <param name="maxDelayMs">Maximum delay cap (default: 30000ms)</param>
    /// <returns>Delay in milliseconds</returns>
    public static int CalculateBackoffDelay(int attempt, int initialDelayMs = DefaultInitialDelayMs, int maxDelayMs = 30000)
    {
        var delay = initialDelayMs * (1 << Math.Min(attempt - 1, 10)); // Cap the shift to prevent overflow
        return Math.Min(delay, maxDelayMs);
    }
}

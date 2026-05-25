namespace Kuestenlogik.Surgewave.Streams.Resilience;

/// <summary>
/// Thrown when all retry attempts have been exhausted.
/// Wraps the original exception and captures diagnostic information.
/// </summary>
public sealed class RetryExhaustedException : Exception
{
    /// <summary>Total number of attempts made (initial attempt + retries).</summary>
    public int Attempts { get; }

    /// <summary>Wall-clock time elapsed across all attempts.</summary>
    public TimeSpan Elapsed { get; }

    /// <inheritdoc/>
    public RetryExhaustedException(int attempts, TimeSpan elapsed, Exception innerException)
        : base(
            $"Retry exhausted after {attempts} attempt(s) over {elapsed.TotalMilliseconds:F0} ms.",
            innerException)
    {
        Attempts = attempts;
        Elapsed = elapsed;
    }
}

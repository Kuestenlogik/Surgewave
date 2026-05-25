namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Suppression configuration for windowed aggregations.
/// </summary>
public sealed class Suppressed<TKey>
{
    public TimeSpan? BufferTime { get; private init; }
    public long? BufferSize { get; private init; }
    public bool UntilWindowCloses { get; private init; }

    private Suppressed() { }

    public static Suppressed<TKey> UntilTimeLimit(TimeSpan duration, long maxBytes)
    {
        return new Suppressed<TKey>
        {
            BufferTime = duration,
            BufferSize = maxBytes,
            UntilWindowCloses = false
        };
    }

    public static Suppressed<TKey> UntilWindowClose(TimeSpan gracePeriod)
    {
        return new Suppressed<TKey>
        {
            BufferTime = gracePeriod,
            UntilWindowCloses = true
        };
    }
}

namespace Kuestenlogik.Surgewave.Streams.EventTime;

/// <summary>
/// Extracts timestamps from elements.
/// </summary>
public interface ITimestampAssigner<T>
{
    /// <summary>
    /// Extracts the timestamp from an element.
    /// </summary>
    /// <param name="element">The element to extract the timestamp from.</param>
    /// <param name="recordTimestamp">The timestamp from the record metadata (e.g., Kafka timestamp).</param>
    /// <returns>The event timestamp in milliseconds since epoch.</returns>
    long ExtractTimestamp(T element, long recordTimestamp);
}

/// <summary>
/// Lambda-based timestamp assigner.
/// </summary>
public sealed class LambdaTimestampAssigner<T> : ITimestampAssigner<T>
{
    private readonly Func<T, long, long> _extractor;

    public LambdaTimestampAssigner(Func<T, long, long> extractor)
    {
        _extractor = extractor;
    }

    public long ExtractTimestamp(T element, long recordTimestamp)
    {
        return _extractor(element, recordTimestamp);
    }
}

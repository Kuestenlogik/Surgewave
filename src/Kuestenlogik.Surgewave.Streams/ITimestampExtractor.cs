namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Extracts timestamps from stream records for event-time processing.
/// </summary>
public interface ITimestampExtractor
{
    /// <summary>Extracts the timestamp from a record.</summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="record">The stream record.</param>
    /// <param name="previousTimestamp">The timestamp of the previously processed record.</param>
    /// <returns>The extracted timestamp in Unix milliseconds.</returns>
    long Extract<TKey, TValue>(StreamRecord<TKey, TValue> record, long previousTimestamp);
}

/// <summary>
/// Uses the record's embedded timestamp.
/// </summary>
public sealed class RecordTimestampExtractor : ITimestampExtractor
{
    public static readonly RecordTimestampExtractor Instance = new();

    public long Extract<TKey, TValue>(StreamRecord<TKey, TValue> record, long previousTimestamp)
        => record.Timestamp;
}

/// <summary>
/// Uses wall-clock time.
/// </summary>
public sealed class WallClockTimestampExtractor : ITimestampExtractor
{
    public static readonly WallClockTimestampExtractor Instance = new();

    public long Extract<TKey, TValue>(StreamRecord<TKey, TValue> record, long previousTimestamp)
        => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

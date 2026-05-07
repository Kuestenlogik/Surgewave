namespace Confluent.Kafka;

/// <summary>
/// Type of timestamp.
/// </summary>
public enum TimestampType
{
    /// <summary>Timestamp not available.</summary>
    NotAvailable = 0,

    /// <summary>Timestamp set by the producer.</summary>
    CreateTime = 1,

    /// <summary>Timestamp set by the broker on append.</summary>
    LogAppendTime = 2
}

/// <summary>
/// Represents a Kafka message timestamp.
/// </summary>
public readonly struct Timestamp : IEquatable<Timestamp>
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// Default timestamp (not available).
    /// </summary>
    public static Timestamp Default { get; } = new(0, TimestampType.NotAvailable);

    /// <summary>
    /// Creates a new Timestamp.
    /// </summary>
    public Timestamp(long unixTimestampMs, TimestampType type = TimestampType.CreateTime)
    {
        UnixTimestampMs = unixTimestampMs;
        Type = type;
    }

    /// <summary>
    /// Creates a new Timestamp from a DateTime.
    /// </summary>
    public Timestamp(DateTime dateTime, TimestampType type = TimestampType.CreateTime)
    {
        UnixTimestampMs = (long)(dateTime.ToUniversalTime() - UnixEpoch).TotalMilliseconds;
        Type = type;
    }

    /// <summary>
    /// Creates a new Timestamp from a DateTimeOffset.
    /// </summary>
    public Timestamp(DateTimeOffset dateTimeOffset, TimestampType type = TimestampType.CreateTime)
    {
        UnixTimestampMs = dateTimeOffset.ToUnixTimeMilliseconds();
        Type = type;
    }

    /// <summary>
    /// The Unix timestamp in milliseconds.
    /// </summary>
    public long UnixTimestampMs { get; }

    /// <summary>
    /// The type of timestamp.
    /// </summary>
    public TimestampType Type { get; }

    /// <summary>
    /// Converts to UTC DateTime.
    /// </summary>
    public DateTime UtcDateTime => UnixEpoch.AddMilliseconds(UnixTimestampMs);

    /// <summary>
    /// Converts to DateTimeOffset.
    /// </summary>
    public DateTimeOffset DateTimeOffset => DateTimeOffset.FromUnixTimeMilliseconds(UnixTimestampMs);

    /// <inheritdoc/>
    public bool Equals(Timestamp other) => UnixTimestampMs == other.UnixTimestampMs && Type == other.Type;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Timestamp other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(UnixTimestampMs, Type);

    /// <inheritdoc/>
    public override string ToString() => $"{UtcDateTime:O} ({Type})";

    /// <summary>Equality operator.</summary>
    public static bool operator ==(Timestamp left, Timestamp right) => left.Equals(right);

    /// <summary>Inequality operator.</summary>
    public static bool operator !=(Timestamp left, Timestamp right) => !left.Equals(right);
}

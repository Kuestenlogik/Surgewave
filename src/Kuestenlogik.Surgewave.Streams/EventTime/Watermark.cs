namespace Kuestenlogik.Surgewave.Streams.EventTime;

/// <summary>
/// Represents a watermark - a timestamp that indicates no events with a timestamp
/// less than or equal to the watermark will arrive after it.
/// </summary>
public readonly struct Watermark : IEquatable<Watermark>, IComparable<Watermark>
{
    /// <summary>
    /// Special watermark indicating no watermark has been set.
    /// </summary>
    public static readonly Watermark None = new(long.MinValue);

    /// <summary>
    /// Special watermark indicating the end of time (no more events expected).
    /// </summary>
    public static readonly Watermark Max = new(long.MaxValue);

    /// <summary>
    /// The timestamp of this watermark in milliseconds since epoch.
    /// </summary>
    public long Timestamp { get; }

    public Watermark(long timestamp)
    {
        Timestamp = timestamp;
    }

    public static Watermark FromTimestamp(long timestamp) => new(timestamp);
    public static Watermark FromDateTime(DateTimeOffset dateTime) => new(dateTime.ToUnixTimeMilliseconds());

    public bool IsNone => Timestamp == long.MinValue;
    public bool IsMax => Timestamp == long.MaxValue;

    public int CompareTo(Watermark other) => Timestamp.CompareTo(other.Timestamp);
    public bool Equals(Watermark other) => Timestamp == other.Timestamp;
    public override bool Equals(object? obj) => obj is Watermark other && Equals(other);
    public override int GetHashCode() => Timestamp.GetHashCode();
    public override string ToString() => IsNone ? "Watermark.None" : IsMax ? "Watermark.Max" : $"Watermark({Timestamp})";

    public static bool operator ==(Watermark left, Watermark right) => left.Equals(right);
    public static bool operator !=(Watermark left, Watermark right) => !left.Equals(right);
    public static bool operator <(Watermark left, Watermark right) => left.Timestamp < right.Timestamp;
    public static bool operator >(Watermark left, Watermark right) => left.Timestamp > right.Timestamp;
    public static bool operator <=(Watermark left, Watermark right) => left.Timestamp <= right.Timestamp;
    public static bool operator >=(Watermark left, Watermark right) => left.Timestamp >= right.Timestamp;
}

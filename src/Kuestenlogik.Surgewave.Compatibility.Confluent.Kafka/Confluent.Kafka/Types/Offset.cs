namespace Confluent.Kafka;

/// <summary>
/// Represents an offset within a Kafka partition.
/// </summary>
public readonly struct Offset : IEquatable<Offset>, IComparable<Offset>
{
    /// <summary>
    /// Special offset indicating the beginning of a partition.
    /// </summary>
    public static Offset Beginning { get; } = new(-2);

    /// <summary>
    /// Special offset indicating the end of a partition.
    /// </summary>
    public static Offset End { get; } = new(-1);

    /// <summary>
    /// Special offset indicating that no offset is stored.
    /// </summary>
    public static Offset Stored { get; } = new(-1000);

    /// <summary>
    /// Special offset for invalid/unset offset.
    /// </summary>
    public static Offset Unset { get; } = new(-1001);

    /// <summary>
    /// Creates a new Offset with the specified value.
    /// </summary>
    public Offset(long value) => Value = value;

    /// <summary>
    /// The offset value.
    /// </summary>
    public long Value { get; }

    /// <summary>
    /// Whether this is a special offset (Beginning, End, Stored, Unset).
    /// </summary>
    public bool IsSpecial => Value < 0;

    /// <inheritdoc/>
    public bool Equals(Offset other) => Value == other.Value;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Offset other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => Value.GetHashCode();

    /// <inheritdoc/>
    public int CompareTo(Offset other) => Value.CompareTo(other.Value);

    /// <inheritdoc/>
    public override string ToString() => Value switch
    {
        -2 => "Beginning",
        -1 => "End",
        -1000 => "Stored",
        -1001 => "Unset",
        _ when Value < 0 => $"[{Value}]",
        _ => Value.ToString()
    };

    /// <summary>Implicit conversion from long.</summary>
    public static implicit operator Offset(long value) => new(value);

    /// <summary>Implicit conversion to long.</summary>
    public static implicit operator long(Offset offset) => offset.Value;

    /// <summary>Equality operator.</summary>
    public static bool operator ==(Offset left, Offset right) => left.Equals(right);

    /// <summary>Inequality operator.</summary>
    public static bool operator !=(Offset left, Offset right) => !left.Equals(right);

    /// <summary>Less than operator.</summary>
    public static bool operator <(Offset left, Offset right) => left.CompareTo(right) < 0;

    /// <summary>Greater than operator.</summary>
    public static bool operator >(Offset left, Offset right) => left.CompareTo(right) > 0;

    /// <summary>Less than or equal operator.</summary>
    public static bool operator <=(Offset left, Offset right) => left.CompareTo(right) <= 0;

    /// <summary>Greater than or equal operator.</summary>
    public static bool operator >=(Offset left, Offset right) => left.CompareTo(right) >= 0;

    /// <summary>Addition operator.</summary>
    public static Offset operator +(Offset offset, long value) => new(offset.Value + value);

    /// <summary>Subtraction operator.</summary>
    public static Offset operator -(Offset offset, long value) => new(offset.Value - value);
}

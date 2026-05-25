namespace Confluent.Kafka;

/// <summary>
/// Represents a Kafka partition.
/// </summary>
public readonly struct Partition : IEquatable<Partition>, IComparable<Partition>
{
    /// <summary>
    /// Special partition value indicating that any partition may be used.
    /// </summary>
    public static Partition Any { get; } = new(-1);

    /// <summary>
    /// Creates a new Partition with the specified value.
    /// </summary>
    public Partition(int value) => Value = value;

    /// <summary>
    /// The partition value.
    /// </summary>
    public int Value { get; }

    /// <summary>
    /// Whether this is the Any partition.
    /// </summary>
    public bool IsSpecial => Value < 0;

    /// <inheritdoc/>
    public bool Equals(Partition other) => Value == other.Value;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Partition other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => Value;

    /// <inheritdoc/>
    public int CompareTo(Partition other) => Value.CompareTo(other.Value);

    /// <inheritdoc/>
    public override string ToString() => IsSpecial ? $"[{Value}]" : Value.ToString();

    /// <summary>Implicit conversion from int.</summary>
    public static implicit operator Partition(int value) => new(value);

    /// <summary>Implicit conversion to int.</summary>
    public static implicit operator int(Partition partition) => partition.Value;

    /// <summary>Equality operator.</summary>
    public static bool operator ==(Partition left, Partition right) => left.Equals(right);

    /// <summary>Inequality operator.</summary>
    public static bool operator !=(Partition left, Partition right) => !left.Equals(right);

    /// <summary>Less than operator.</summary>
    public static bool operator <(Partition left, Partition right) => left.CompareTo(right) < 0;

    /// <summary>Greater than operator.</summary>
    public static bool operator >(Partition left, Partition right) => left.CompareTo(right) > 0;

    /// <summary>Less than or equal operator.</summary>
    public static bool operator <=(Partition left, Partition right) => left.CompareTo(right) <= 0;

    /// <summary>Greater than or equal operator.</summary>
    public static bool operator >=(Partition left, Partition right) => left.CompareTo(right) >= 0;
}

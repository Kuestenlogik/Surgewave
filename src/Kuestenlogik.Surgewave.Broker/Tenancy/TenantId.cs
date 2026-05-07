namespace Kuestenlogik.Surgewave.Broker.Tenancy;

/// <summary>
/// Strongly-typed tenant identifier. Immutable, case-insensitive comparison.
/// </summary>
public readonly record struct TenantId(string Value) : IComparable<TenantId>
{
    /// <summary>Default tenant for backward compatibility with non-tenant-aware clients.</summary>
    public static readonly TenantId Default = new("default");

    public bool IsDefault => string.Equals(Value, "default", StringComparison.OrdinalIgnoreCase);

    public int CompareTo(TenantId other) =>
        string.Compare(Value, other.Value, StringComparison.OrdinalIgnoreCase);

    public bool Equals(TenantId other) =>
        string.Equals(Value, other.Value, StringComparison.OrdinalIgnoreCase);

    public override int GetHashCode() =>
        StringComparer.OrdinalIgnoreCase.GetHashCode(Value);

    public override string ToString() => Value;

    public static bool operator <(TenantId left, TenantId right) => left.CompareTo(right) < 0;
    public static bool operator >(TenantId left, TenantId right) => left.CompareTo(right) > 0;
    public static bool operator <=(TenantId left, TenantId right) => left.CompareTo(right) <= 0;
    public static bool operator >=(TenantId left, TenantId right) => left.CompareTo(right) >= 0;

    /// <summary>Validates tenant ID format: alphanumeric, hyphens, underscores, 1-64 chars.</summary>
    public static bool IsValid(string value) =>
        !string.IsNullOrWhiteSpace(value) &&
        value.Length <= 64 &&
        value.All(c => char.IsLetterOrDigit(c) || c == '-' || c == '_');
}

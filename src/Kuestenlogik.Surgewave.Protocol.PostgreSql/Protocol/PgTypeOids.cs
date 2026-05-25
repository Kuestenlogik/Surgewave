namespace Kuestenlogik.Surgewave.Protocol.PostgreSql.Protocol;

/// <summary>
/// PostgreSQL type OID constants used in RowDescription messages.
/// Maps common data types to their PostgreSQL OIDs so that clients
/// can correctly interpret column types.
/// </summary>
internal static class PgTypeOids
{
    public const int Bool = 16;
    public const int Bytea = 17;
    public const int Int2 = 21;
    public const int Int4 = 23;
    public const int Int8 = 20;
    public const int Float4 = 700;
    public const int Float8 = 701;
    public const int Numeric = 1700;
    public const int Text = 25;
    public const int Varchar = 1043;
    public const int Timestamp = 1114;
    public const int TimestampTz = 1184;
    public const int Date = 1082;
    public const int Json = 114;
    public const int Jsonb = 3802;
    public const int Uuid = 2950;

    /// <summary>
    /// Returns the PostgreSQL type OID for the given CLR value.
    /// Falls back to <see cref="Text"/> for unknown types.
    /// </summary>
    public static int FromClrValue(object? value) => value switch
    {
        null => Text,
        bool => Bool,
        byte[] => Bytea,
        short => Int2,
        int => Int4,
        long => Int8,
        float => Float4,
        double => Float8,
        decimal => Numeric,
        DateTime => TimestampTz,
        DateTimeOffset => TimestampTz,
        DateOnly => Date,
        Guid => Uuid,
        _ => Text,
    };

    /// <summary>
    /// Returns the type length for a given OID (-1 for variable-length types).
    /// </summary>
    public static short TypeLength(int oid) => oid switch
    {
        Bool => 1,
        Int2 => 2,
        Int4 => 4,
        Int8 => 8,
        Float4 => 4,
        Float8 => 8,
        Uuid => 16,
        _ => -1,
    };
}

namespace Kuestenlogik.Surgewave.Streams.DataFrame;

/// <summary>
/// Specification for window functions in DataFrames.
/// </summary>
public sealed class WindowSpec
{
    private readonly List<string> _partitionBy = new();
    private readonly List<(string Column, bool Ascending)> _orderBy = new();
    private WindowFrame? _frame;

    /// <summary>
    /// Defines the partition columns.
    /// </summary>
    public WindowSpec PartitionBy(params string[] columns)
    {
        _partitionBy.AddRange(columns);
        return this;
    }

    /// <summary>
    /// Defines the order within partitions (ascending).
    /// </summary>
    public WindowSpec OrderBy(params string[] columns)
    {
        foreach (var col in columns)
            _orderBy.Add((col, true));
        return this;
    }

    /// <summary>
    /// Defines descending order.
    /// </summary>
    public WindowSpec OrderByDesc(params string[] columns)
    {
        foreach (var col in columns)
            _orderBy.Add((col, false));
        return this;
    }

    /// <summary>
    /// Specifies a row-based window frame.
    /// </summary>
    public WindowSpec RowsBetween(long start, long end)
    {
        _frame = new WindowFrame(WindowFrameType.Rows, start, end);
        return this;
    }

    /// <summary>
    /// Specifies a range-based window frame.
    /// </summary>
    public WindowSpec RangeBetween(long start, long end)
    {
        _frame = new WindowFrame(WindowFrameType.Range, start, end);
        return this;
    }

    public override string ToString()
    {
        var parts = new List<string>();

        if (_partitionBy.Count > 0)
            parts.Add($"PARTITION BY {string.Join(", ", _partitionBy)}");

        if (_orderBy.Count > 0)
            parts.Add($"ORDER BY {string.Join(", ", _orderBy.Select(o => o.Ascending ? o.Column : $"{o.Column} DESC"))}");

        if (_frame != null)
            parts.Add(_frame.ToString());

        return string.Join(" ", parts);
    }

    /// <summary>
    /// Special value representing unbounded preceding.
    /// </summary>
    public static long UnboundedPreceding => long.MinValue;

    /// <summary>
    /// Special value representing unbounded following.
    /// </summary>
    public static long UnboundedFollowing => long.MaxValue;

    /// <summary>
    /// Special value representing the current row.
    /// </summary>
    public static long CurrentRow => 0;
}

/// <summary>
/// Factory for creating window specifications.
/// </summary>
public static class Window
{
    /// <summary>
    /// Creates a new window specification with partition columns.
    /// </summary>
    public static WindowSpec PartitionBy(params string[] columns)
    {
        return new WindowSpec().PartitionBy(columns);
    }

    /// <summary>
    /// Creates a new window specification with order columns.
    /// </summary>
    public static WindowSpec OrderBy(params string[] columns)
    {
        return new WindowSpec().OrderBy(columns);
    }

    /// <summary>
    /// Creates a row-number window function.
    /// </summary>
    public static Column RowNumber() => new Column("ROW_NUMBER()");

    /// <summary>
    /// Creates a rank window function.
    /// </summary>
    public static Column Rank() => new Column("RANK()");

    /// <summary>
    /// Creates a dense rank window function.
    /// </summary>
    public static Column DenseRank() => new Column("DENSE_RANK()");

    /// <summary>
    /// Creates a percent rank window function.
    /// </summary>
    public static Column PercentRank() => new Column("PERCENT_RANK()");

    /// <summary>
    /// Creates a NTILE window function.
    /// </summary>
    public static Column NTile(int n) => new Column($"NTILE({n})");

    /// <summary>
    /// Creates a LAG window function.
    /// </summary>
    public static Column Lag(string columnName, int offset = 1, object? defaultValue = null)
    {
        return defaultValue == null
            ? new Column($"LAG({columnName}, {offset})")
            : new Column($"LAG({columnName}, {offset}, {defaultValue})");
    }

    /// <summary>
    /// Creates a LEAD window function.
    /// </summary>
    public static Column Lead(string columnName, int offset = 1, object? defaultValue = null)
    {
        return defaultValue == null
            ? new Column($"LEAD({columnName}, {offset})")
            : new Column($"LEAD({columnName}, {offset}, {defaultValue})");
    }

    public static long UnboundedPreceding => WindowSpec.UnboundedPreceding;
    public static long UnboundedFollowing => WindowSpec.UnboundedFollowing;
    public static long CurrentRow => WindowSpec.CurrentRow;
}

/// <summary>
/// Defines a window frame.
/// </summary>
internal sealed class WindowFrame
{
    public WindowFrameType Type { get; }
    public long Start { get; }
    public long End { get; }

    public WindowFrame(WindowFrameType type, long start, long end)
    {
        Type = type;
        Start = start;
        End = end;
    }

    public override string ToString()
    {
        var frameType = Type == WindowFrameType.Rows ? "ROWS" : "RANGE";
        var startStr = Start == long.MinValue ? "UNBOUNDED PRECEDING" :
                       Start == 0 ? "CURRENT ROW" :
                       Start < 0 ? $"{-Start} PRECEDING" : $"{Start} FOLLOWING";
        var endStr = End == long.MaxValue ? "UNBOUNDED FOLLOWING" :
                     End == 0 ? "CURRENT ROW" :
                     End < 0 ? $"{-End} PRECEDING" : $"{End} FOLLOWING";
        return $"{frameType} BETWEEN {startStr} AND {endStr}";
    }
}

internal enum WindowFrameType
{
    Rows,
    Range
}

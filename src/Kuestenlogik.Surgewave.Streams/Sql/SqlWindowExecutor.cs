namespace Kuestenlogik.Surgewave.Streams.Sql;

/// <summary>
/// Executes windowed aggregations over a set of rows.
/// Supports tumbling, hopping, and session window types.
/// Groups rows into time-based windows, then applies GROUP BY + aggregation within each window.
/// Result rows include window_start, window_end, group key columns, and aggregate values.
/// </summary>
internal sealed class SqlWindowExecutor
{
    /// <summary>
    /// Execute a windowed aggregation query.
    /// </summary>
    /// <param name="rows">Input rows with a timestamp column.</param>
    /// <param name="window">Window specification (type, time column, size, optional advance).</param>
    /// <param name="groupByKeys">GROUP BY expressions (evaluated per row).</param>
    /// <param name="selectItems">SELECT items including aggregate functions.</param>
    /// <param name="evaluator">Expression evaluator for resolving expressions against rows.</param>
    /// <returns>Result rows with window_start, window_end, group columns, and aggregates.</returns>
    public static List<Dictionary<string, object?>> Execute(
        List<Dictionary<string, object?>> rows,
        WindowSpec window,
        List<SqlExpression>? groupByKeys,
        List<SelectItem> selectItems,
        SqlExpressionEvaluator evaluator)
    {
        // Assign each row to one or more windows
        var windowedGroups = window.Type switch
        {
            WindowType.Tumble => AssignTumblingWindows(rows, window, groupByKeys, evaluator),
            WindowType.Hop => AssignHoppingWindows(rows, window, groupByKeys, evaluator),
            WindowType.Session => AssignSessionWindows(rows, window, groupByKeys, evaluator),
            _ => throw new SqlParseException($"Unsupported window type: {window.Type}")
        };

        // Aggregate each window group
        var results = new List<Dictionary<string, object?>>();
        foreach (var group in windowedGroups)
        {
            var resultRow = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["window_start"] = group.WindowStart,
                ["window_end"] = group.WindowEnd
            };

            // Add group-by key columns
            if (groupByKeys is { Count: > 0 })
            {
                var firstRow = group.Rows[0];
                foreach (var expr in groupByKeys)
                {
                    if (expr is ColumnRef col)
                    {
                        resultRow[col.Column] = evaluator.Evaluate(expr, firstRow);
                    }
                }
            }

            // Evaluate SELECT items (resolve aggregates)
            foreach (var item in selectItems)
            {
                if (item is ExpressionSelectItem exprItem)
                {
                    var value = ResolveAggregate(exprItem.Expression, group.Rows, evaluator);
                    var colName = exprItem.Alias ?? GetExpressionName(exprItem.Expression);
                    resultRow[colName] = value;
                }
            }

            results.Add(resultRow);
        }

        return results;
    }

    // === Window Assignment ===

    private static List<WindowGroup> AssignTumblingWindows(
        List<Dictionary<string, object?>> rows,
        WindowSpec window,
        List<SqlExpression>? groupByKeys,
        SqlExpressionEvaluator evaluator)
    {
        var windowSize = window.Size;
        var groups = new Dictionary<string, WindowGroup>(StringComparer.Ordinal);

        foreach (var row in rows)
        {
            var timestamp = ExtractTimestamp(row, window.TimeColumn);
            if (timestamp == null) continue;

            var windowStart = GetTumblingWindowStart(timestamp.Value, windowSize);
            var windowEnd = windowStart + windowSize;
            var groupKey = BuildGroupKey(row, groupByKeys, evaluator);
            var compositeKey = $"{windowStart.Ticks}|{groupKey}";

            if (!groups.TryGetValue(compositeKey, out var group))
            {
                group = new WindowGroup(windowStart, windowEnd);
                groups[compositeKey] = group;
            }
            group.Rows.Add(row);
        }

        return [.. groups.Values];
    }

    private static List<WindowGroup> AssignHoppingWindows(
        List<Dictionary<string, object?>> rows,
        WindowSpec window,
        List<SqlExpression>? groupByKeys,
        SqlExpressionEvaluator evaluator)
    {
        var windowSize = window.Size;
        var advance = window.Advance ?? window.Size;
        var groups = new Dictionary<string, WindowGroup>(StringComparer.Ordinal);

        foreach (var row in rows)
        {
            var timestamp = ExtractTimestamp(row, window.TimeColumn);
            if (timestamp == null) continue;

            var groupKey = BuildGroupKey(row, groupByKeys, evaluator);

            // A row can fall into multiple hopping windows
            var earliest = GetEarliestHoppingWindowStart(timestamp.Value, windowSize, advance);
            for (var start = earliest; start + windowSize > timestamp.Value; start -= advance)
            {
                // Only include windows where this row actually falls within
                if (timestamp.Value >= start && timestamp.Value < start + windowSize)
                {
                    var compositeKey = $"{start.Ticks}|{groupKey}";
                    if (!groups.TryGetValue(compositeKey, out var group))
                    {
                        group = new WindowGroup(start, start + windowSize);
                        groups[compositeKey] = group;
                    }
                    group.Rows.Add(row);
                }
            }
        }

        return [.. groups.Values];
    }

    private static List<WindowGroup> AssignSessionWindows(
        List<Dictionary<string, object?>> rows,
        WindowSpec window,
        List<SqlExpression>? groupByKeys,
        SqlExpressionEvaluator evaluator)
    {
        // Session windows: group by key, then merge events within the gap
        var keyedRows = new Dictionary<string, List<(DateTimeOffset Timestamp, Dictionary<string, object?> Row)>>(StringComparer.Ordinal);

        foreach (var row in rows)
        {
            var timestamp = ExtractTimestamp(row, window.TimeColumn);
            if (timestamp == null) continue;

            var groupKey = BuildGroupKey(row, groupByKeys, evaluator);
            if (!keyedRows.TryGetValue(groupKey, out var list))
            {
                list = [];
                keyedRows[groupKey] = list;
            }
            list.Add((timestamp.Value, row));
        }

        var results = new List<WindowGroup>();

        foreach (var (_, events) in keyedRows)
        {
            // Sort by timestamp
            events.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));

            if (events.Count == 0) continue;

            var currentStart = events[0].Timestamp;
            var currentEnd = events[0].Timestamp + window.Size;
            var currentRows = new List<Dictionary<string, object?>> { events[0].Row };

            for (var i = 1; i < events.Count; i++)
            {
                var (ts, row) = events[i];
                if (ts <= currentEnd)
                {
                    // Within session gap — extend
                    currentEnd = ts + window.Size;
                    currentRows.Add(row);
                }
                else
                {
                    // Gap exceeded — close current session and start new one
                    results.Add(new WindowGroup(currentStart, currentEnd) { Rows = currentRows });
                    currentStart = ts;
                    currentEnd = ts + window.Size;
                    currentRows = [row];
                }
            }

            results.Add(new WindowGroup(currentStart, currentEnd) { Rows = currentRows });
        }

        return results;
    }

    // === Helpers ===

    private static DateTimeOffset? ExtractTimestamp(Dictionary<string, object?> row, string timeColumn)
    {
        if (!row.TryGetValue(timeColumn, out var val) || val == null) return null;

        return val switch
        {
            DateTimeOffset dto => dto,
            DateTime dt => new DateTimeOffset(dt),
            long ticks => DateTimeOffset.FromUnixTimeMilliseconds(ticks),
            double d => DateTimeOffset.FromUnixTimeMilliseconds((long)d),
            string s when DateTimeOffset.TryParse(s, out var parsed) => parsed,
            string s when long.TryParse(s, out var ms) => DateTimeOffset.FromUnixTimeMilliseconds(ms),
            _ => null
        };
    }

    private static DateTimeOffset GetTumblingWindowStart(DateTimeOffset timestamp, TimeSpan windowSize)
    {
        var ticks = timestamp.UtcTicks;
        var windowTicks = windowSize.Ticks;
        var windowStart = ticks - (ticks % windowTicks);
        return new DateTimeOffset(windowStart, TimeSpan.Zero);
    }

    private static DateTimeOffset GetEarliestHoppingWindowStart(DateTimeOffset timestamp, TimeSpan windowSize, TimeSpan advance)
    {
        // The latest window that could contain this timestamp
        var ticks = timestamp.UtcTicks;
        var advanceTicks = advance.Ticks;
        var latestStart = ticks - (ticks % advanceTicks);
        // Walk back to find the earliest window that still contains this timestamp
        var earliest = latestStart;
        while (earliest > DateTimeOffset.MinValue.UtcTicks && timestamp.UtcTicks < earliest + windowSize.Ticks)
        {
            earliest -= advanceTicks;
        }
        return new DateTimeOffset(earliest + advanceTicks, TimeSpan.Zero);
    }

    private static string BuildGroupKey(
        Dictionary<string, object?> row,
        List<SqlExpression>? groupByKeys,
        SqlExpressionEvaluator evaluator)
    {
        if (groupByKeys is not { Count: > 0 }) return "__all__";
        var parts = groupByKeys.Select(expr => evaluator.Evaluate(expr, row)?.ToString() ?? "NULL");
        return string.Join("|", parts);
    }

    private static object? ResolveAggregate(
        SqlExpression expr,
        List<Dictionary<string, object?>> groupRows,
        SqlExpressionEvaluator evaluator)
    {
        if (expr is FunctionCall func)
        {
            var argExpr = func.Arguments.Count > 0 ? func.Arguments[0] : null;
            var values = argExpr != null
                ? groupRows.Select(r => evaluator.Evaluate(argExpr, r)).ToList()
                : groupRows.Select(_ => (object?)1).ToList();

            return func.Name switch
            {
                "COUNT" => func.Distinct
                    ? values.Where(v => v != null).Distinct().Count()
                    : func.Arguments.Count == 0
                        ? groupRows.Count
                        : values.Count(v => v != null),
                "SUM" => values.Where(v => v != null).Sum(v => SqlExpressionEvaluator.ToDouble(v)),
                "AVG" => values.Where(v => v != null).DefaultIfEmpty(null)
                    .Average(v => v != null ? SqlExpressionEvaluator.ToDouble(v) : 0),
                "MIN" => values.Where(v => v != null).MinBy(v => SqlExpressionEvaluator.ToDouble(v)),
                "MAX" => values.Where(v => v != null).MaxBy(v => SqlExpressionEvaluator.ToDouble(v)),
                _ => evaluator.EvaluateFunction(func, groupRows[0])
            };
        }

        // For non-aggregate expressions, evaluate against first row
        if (expr is ColumnRef)
            return evaluator.Evaluate(expr, groupRows[0]);

        // Binary expressions might contain aggregates
        if (expr is BinaryOp bin)
        {
            var left = ResolveAggregate(bin.Left, groupRows, evaluator);
            var right = ResolveAggregate(bin.Right, groupRows, evaluator);
            return bin.Op switch
            {
                BinaryOperator.Plus => SqlExpressionEvaluator.ToDouble(left) + SqlExpressionEvaluator.ToDouble(right),
                BinaryOperator.Minus => SqlExpressionEvaluator.ToDouble(left) - SqlExpressionEvaluator.ToDouble(right),
                BinaryOperator.Multiply => SqlExpressionEvaluator.ToDouble(left) * SqlExpressionEvaluator.ToDouble(right),
                BinaryOperator.Divide => SqlExpressionEvaluator.ToDouble(right) is not 0.0
                    ? SqlExpressionEvaluator.ToDouble(left) / SqlExpressionEvaluator.ToDouble(right)
                    : double.NaN,
                _ => evaluator.Evaluate(expr, groupRows[0])
            };
        }

        return evaluator.Evaluate(expr, groupRows[0]);
    }

    private static string GetExpressionName(SqlExpression expr) => expr switch
    {
        ColumnRef col => col.Table != null ? $"{col.Table}.{col.Column}" : col.Column,
        FunctionCall func => $"{func.Name}({string.Join(", ", func.Arguments.Select(GetExpressionName))})",
        _ => expr.ToString() ?? "expr"
    };

    /// <summary>
    /// Internal grouping structure representing rows within a single window.
    /// </summary>
    private sealed class WindowGroup(DateTimeOffset windowStart, DateTimeOffset windowEnd)
    {
        public DateTimeOffset WindowStart { get; } = windowStart;
        public DateTimeOffset WindowEnd { get; } = windowEnd;
        public List<Dictionary<string, object?>> Rows { get; set; } = [];
    }
}

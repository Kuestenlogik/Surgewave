using System.Text.Json;
using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;

namespace Kuestenlogik.Surgewave.Streams.Sql;

/// <summary>
/// Streaming SQL Engine — executes SQL queries on streams of JSON records.
/// Supports SELECT (filter, project, aggregate), windowed aggregations, and joins.
///
/// Usage:
///   var engine = new SqlEngine();
///   engine.RegisterStream("orders", ordersEnumerable);
///   var results = engine.Execute("SELECT customer, SUM(amount) FROM orders GROUP BY customer");
/// </summary>
public sealed class SqlEngine
{
    private readonly Dictionary<string, IEnumerable<Dictionary<string, object?>>> _sources = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, Func<IEnumerable<Dictionary<string, object?>>>> _lazySourceFactories = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, SqlTopicSource> _topicSources = new(StringComparer.OrdinalIgnoreCase);
    private readonly SqlExpressionEvaluator _evaluator = new();
    private readonly MaterializedViewRegistry? _viewRegistry;

    private static readonly Regex AsSelectRegex = new(
        @"\bAS\b\s*",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    public SqlEngine() { }

    /// <summary>
    /// Creates a SqlEngine bound to a materialized view registry.
    /// CREATE / DROP MATERIALIZED VIEW statements operate on the registry,
    /// and SELECT against a registered view returns the view's current snapshot.
    /// </summary>
    public SqlEngine(MaterializedViewRegistry? viewRegistry)
    {
        _viewRegistry = viewRegistry;
    }

    /// <summary>
    /// Register a named stream source with pre-parsed rows.
    /// </summary>
    public void RegisterStream(string name, IEnumerable<Dictionary<string, object?>> rows)
    {
        _sources[name] = rows;
    }

    /// <summary>
    /// Register a named stream source from JSON strings.
    /// </summary>
    public void RegisterJsonStream(string name, IEnumerable<string> jsonLines)
    {
        _lazySourceFactories[name] = () => jsonLines.Select(ParseJsonRow);
    }

    /// <summary>
    /// Register a lazy source factory (evaluated on query execution).
    /// </summary>
    public void RegisterSourceFactory(string name, Func<IEnumerable<Dictionary<string, object?>>> factory)
    {
        _lazySourceFactories[name] = factory;
    }

    /// <summary>
    /// Register a topic source that reads messages from a Surgewave topic.
    /// The topic source yields rows with metadata columns (_offset, _partition, _timestamp, _key).
    /// </summary>
    public void RegisterTopicSource(string name, SqlTopicSource source)
    {
        _topicSources[name] = source;
    }

    /// <summary>
    /// Convenience method: parse SQL, auto-register topic sources from FROM clause
    /// using the provided message reader function, then execute.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="topicReader">
    /// Function that given a topic name returns raw messages.
    /// This allows the engine to automatically resolve table names to topic sources.
    /// </param>
    /// <returns>Query result.</returns>
    public SqlResult ExecuteWithTopicReader(string sql, Func<string, IEnumerable<RawTopicMessage>> topicReader)
    {
        var statement = SqlParser.Parse(sql);

        // Extract table names from the statement and auto-register them
        var tableNames = ExtractTableNames(statement);
        foreach (var tableName in tableNames)
        {
            if (!_sources.ContainsKey(tableName) &&
                !_lazySourceFactories.ContainsKey(tableName) &&
                !_topicSources.ContainsKey(tableName))
            {
                var source = new SqlTopicSource(() => topicReader(tableName));
                _topicSources[tableName] = source;
            }
        }

        return statement switch
        {
            SelectStatement select => ExecuteSelect(select),
            CreateStreamAsSelect csas => ExecuteCreateStreamAs(csas),
            CreateTableAsSelect ctas => ExecuteCreateTableAs(ctas),
            CreateMaterializedViewAsSelect mv => ExecuteCreateMaterializedView(mv, sql),
            DropMaterializedView drop => ExecuteDropMaterializedView(drop),
            InsertIntoSelect insert => ExecuteInsertInto(insert),
            _ => throw new SqlParseException($"Unsupported statement type: {statement.GetType().Name}")
        };
    }

    /// <summary>
    /// Parse and execute a SQL statement. Returns result rows.
    /// </summary>
    public SqlResult Execute(string sql)
    {
        var statement = SqlParser.Parse(sql);
        return statement switch
        {
            SelectStatement select => ExecuteSelect(select),
            CreateStreamAsSelect csas => ExecuteCreateStreamAs(csas),
            CreateTableAsSelect ctas => ExecuteCreateTableAs(ctas),
            CreateMaterializedViewAsSelect mv => ExecuteCreateMaterializedView(mv, sql),
            DropMaterializedView drop => ExecuteDropMaterializedView(drop),
            InsertIntoSelect insert => ExecuteInsertInto(insert),
            _ => throw new SqlParseException($"Unsupported statement type: {statement.GetType().Name}")
        };
    }

    private SqlResult ExecuteSelect(SelectStatement select)
    {
        var rows = ResolveSource(select.Source);

        // WHERE
        if (select.Where != null)
            rows = rows.Where(r => _evaluator.EvaluateCondition(select.Where, r));

        // Detect window spec from JOIN source (e.g., WINDOW TUMBLE(...))
        var windowSpec = ExtractWindowSpec(select.Source);

        // GROUP BY + aggregation (includes projection)
        var hasGroupBy = select.GroupBy is { Count: > 0 };
        var hasWindow = windowSpec != null;

        if (hasWindow)
        {
            // Windowed aggregation: materialize rows and route to SqlWindowExecutor
            var groupByExprs = hasGroupBy ? select.GroupBy!.ToList() : null;
            rows = SqlWindowExecutor.Execute(
                rows.ToList(), windowSpec!, groupByExprs, select.Columns.ToList(), _evaluator);
        }
        else if (hasGroupBy)
        {
            rows = ExecuteGroupBy(rows, select);
        }

        // HAVING (post-aggregation filter)
        if (select.Having != null)
            rows = rows.Where(r => _evaluator.EvaluateCondition(select.Having, r));

        // SELECT projection (skip if GROUP BY or window already projected)
        if (!hasGroupBy && !hasWindow)
            rows = Project(rows, select.Columns);

        // ORDER BY
        if (select.OrderBy is { Count: > 0 })
            rows = ExecuteOrderBy(rows, select.OrderBy);

        // LIMIT
        if (select.Limit.HasValue)
            rows = rows.Take(select.Limit.Value);

        // Materialize
        var resultRows = rows.ToList();
        var columnNames = resultRows.Count > 0
            ? resultRows[0].Keys.ToList()
            : ExtractColumnNames(select.Columns);

        return new SqlResult(resultRows, columnNames, select.EmitChanges);
    }

    private SqlResult ExecuteCreateStreamAs(CreateStreamAsSelect csas)
    {
        var result = ExecuteSelect(csas.Query);
        // Register the result as a new stream source
        _sources[csas.Name] = result.Rows;
        return result with { CreatedName = csas.Name, IsCreateStatement = true };
    }

    private SqlResult ExecuteCreateTableAs(CreateTableAsSelect ctas)
    {
        var result = ExecuteSelect(ctas.Query);
        _sources[ctas.Name] = result.Rows;
        return result with { CreatedName = ctas.Name, IsCreateStatement = true };
    }

    private SqlResult ExecuteCreateMaterializedView(CreateMaterializedViewAsSelect mv, string originalSql)
    {
        if (_viewRegistry is null)
            throw new InvalidOperationException(
                "CREATE MATERIALIZED VIEW requires a SqlEngine bound to a MaterializedViewRegistry. " +
                "Construct the engine via `new SqlEngine(registry)`.");

        var selectSql = ExtractSelectSql(originalSql);
        var sourceTopics = new List<string>();
        CollectTableNames(mv.Query.Source, sourceTopics);

        var keyColumns = new List<string>();
        if (mv.Query.GroupBy is { Count: > 0 })
        {
            foreach (var expr in mv.Query.GroupBy)
                if (expr is ColumnRef col)
                    keyColumns.Add(col.Column);
        }

        var hasAggregation = mv.Query.GroupBy is { Count: > 0 } ||
                             ContainsAggregateFunction(mv.Query.Columns);

        var definition = new ViewDefinition(
            Name: mv.Name,
            OriginalSql: originalSql,
            SelectSql: selectSql,
            SourceTopics: sourceTopics,
            KeyColumns: keyColumns,
            HasAggregation: hasAggregation,
            IfNotExists: mv.IfNotExists,
            CreatedAt: DateTimeOffset.UtcNow);

        if (!_viewRegistry.TryRegister(definition, out _))
            throw new SqlParseException($"Materialized view '{mv.Name}' already exists");

        return new SqlResult([], [], EmitChanges: false)
        {
            CreatedName = mv.Name,
            IsCreateStatement = true
        };
    }

    private SqlResult ExecuteDropMaterializedView(DropMaterializedView drop)
    {
        if (_viewRegistry is null)
            throw new InvalidOperationException(
                "DROP MATERIALIZED VIEW requires a SqlEngine bound to a MaterializedViewRegistry.");

        var removed = _viewRegistry.TryUnregister(drop.Name, out _);
        if (!removed && !drop.IfExists)
            throw new SqlParseException($"Materialized view '{drop.Name}' does not exist");

        return new SqlResult([], [], EmitChanges: false)
        {
            CreatedName = drop.Name,
            IsCreateStatement = true
        };
    }

    /// <summary>
    /// Extracts the SELECT body from a CREATE [MATERIALIZED] [VIEW|TABLE|STREAM] ... AS SELECT ... statement.
    /// We split on the first <c>AS</c> token (case-insensitive, word-boundary).
    /// </summary>
    private static string ExtractSelectSql(string sql)
    {
        var match = AsSelectRegex.Match(sql);
        if (!match.Success)
            return sql;
        var afterAs = sql[(match.Index + match.Length)..].TrimStart();
        return afterAs.TrimEnd().TrimEnd(';');
    }

    private static bool ContainsAggregateFunction(IReadOnlyList<SelectItem> items)
    {
        foreach (var item in items)
        {
            if (item is ExpressionSelectItem ei && ContainsAggregateFunction(ei.Expression))
                return true;
        }
        return false;
    }

    private static bool ContainsAggregateFunction(SqlExpression expr) => expr switch
    {
        FunctionCall fc => IsAggregateFunctionName(fc.Name) ||
                            fc.Arguments.Any(ContainsAggregateFunction),
        BinaryOp bin => ContainsAggregateFunction(bin.Left) || ContainsAggregateFunction(bin.Right),
        UnaryOp un => ContainsAggregateFunction(un.Operand),
        CaseExpression ce => (ce.Operand != null && ContainsAggregateFunction(ce.Operand)) ||
                              ce.WhenClauses.Any(w => ContainsAggregateFunction(w.Condition) ||
                                                     ContainsAggregateFunction(w.Result)) ||
                              (ce.ElseResult != null && ContainsAggregateFunction(ce.ElseResult)),
        CastExpression ca => ContainsAggregateFunction(ca.Expression),
        _ => false
    };

    private static bool IsAggregateFunctionName(string name) => name.ToUpperInvariant() switch
    {
        "COUNT" or "SUM" or "AVG" or "MIN" or "MAX" or
        "COUNT_DISTINCT" or "COLLECT_LIST" or "COLLECT_SET" or
        "STDDEV" or "VARIANCE" => true,
        _ => false
    };

    private SqlResult ExecuteInsertInto(InsertIntoSelect insert)
    {
        var result = ExecuteSelect(insert.Query);
        if (_sources.TryGetValue(insert.Target, out var existing))
        {
            _sources[insert.Target] = existing.Concat(result.Rows).ToList();
        }
        else
        {
            _sources[insert.Target] = result.Rows;
        }
        return result with { CreatedName = insert.Target, IsCreateStatement = true };
    }

    // === Source resolution ===

    private IEnumerable<Dictionary<string, object?>> ResolveSource(SqlSource source) => source switch
    {
        TableSource ts => ResolveTableSource(ts),
        JoinSource js => ResolveJoinSource(js),
        _ => throw new SqlParseException($"Unsupported source type: {source.GetType().Name}")
    };

    private IEnumerable<Dictionary<string, object?>> ResolveTableSource(TableSource ts)
    {
        if (_sources.TryGetValue(ts.Name, out var rows))
            return MaybeAlias(rows, ts.Alias);
        if (_lazySourceFactories.TryGetValue(ts.Name, out var factory))
            return MaybeAlias(factory(), ts.Alias);
        if (_topicSources.TryGetValue(ts.Name, out var topicSource))
            return MaybeAlias(topicSource, ts.Alias);
        if (_viewRegistry is not null && _viewRegistry.TryGet(ts.Name, out var view))
            return MaybeAlias(view.Snapshot.Rows, ts.Alias);
        throw new SqlParseException($"Unknown source: {ts.Name}");
    }

    private static IEnumerable<Dictionary<string, object?>> MaybeAlias(
        IEnumerable<Dictionary<string, object?>> rows, string? alias)
    {
        if (alias == null) return rows;
        return rows.Select(r =>
        {
            var aliased = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var (key, value) in r)
            {
                aliased[key] = value;
                aliased[$"{alias}.{key}"] = value;
            }
            return aliased;
        });
    }

    private IEnumerable<Dictionary<string, object?>> ResolveJoinSource(JoinSource js)
    {
        var leftRows = ResolveSource(js.Left).ToList();
        var rightRows = ResolveSource(js.Right).ToList();
        var result = new List<Dictionary<string, object?>>();

        foreach (var left in leftRows)
        {
            var matched = false;
            foreach (var right in rightRows)
            {
                var merged = MergeRows(left, right);
                if (_evaluator.EvaluateCondition(js.On, merged))
                {
                    result.Add(merged);
                    matched = true;
                }
            }

            if (!matched && js.JoinType is JoinType.Left or JoinType.Full)
            {
                // Create result with all left columns + null for all right columns
                var merged = new Dictionary<string, object?>(left, StringComparer.OrdinalIgnoreCase);
                var firstRight = rightRows.FirstOrDefault();
                if (firstRight != null)
                {
                    foreach (var key in firstRight.Keys)
                    {
                        if (!merged.ContainsKey(key))
                            merged[key] = null;
                    }
                }
                result.Add(merged);
            }
        }

        if (js.JoinType is JoinType.Right or JoinType.Full)
        {
            foreach (var right in rightRows)
            {
                var matchedAny = leftRows.Any(left =>
                    _evaluator.EvaluateCondition(js.On, MergeRows(left, right)));
                if (!matchedAny)
                {
                    var merged = new Dictionary<string, object?>(right, StringComparer.OrdinalIgnoreCase);
                    var firstLeft = leftRows.FirstOrDefault();
                    if (firstLeft != null)
                        foreach (var key in firstLeft.Keys)
                            merged.TryAdd(key, null);
                    result.Add(merged);
                }
            }
        }

        return result;
    }

    private static Dictionary<string, object?> MergeRows(
        Dictionary<string, object?> left, Dictionary<string, object?> right)
    {
        var merged = new Dictionary<string, object?>(left, StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in right)
            merged.TryAdd(key, value);
        return merged;
    }

    // === GROUP BY + Aggregation ===

    private IEnumerable<Dictionary<string, object?>> ExecuteGroupBy(
        IEnumerable<Dictionary<string, object?>> rows,
        SelectStatement select)
    {
        var groupByExprs = select.GroupBy!;
        var materialized = rows.ToList();

        // Group rows by group key
        var groups = materialized.GroupBy(row =>
        {
            var keyParts = groupByExprs.Select(expr => _evaluator.Evaluate(expr, row)?.ToString() ?? "NULL");
            return string.Join("|", keyParts);
        });

        return groups.Select(group =>
        {
            var firstRow = group.First();
            var resultRow = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            // Add group-by columns
            foreach (var expr in groupByExprs)
            {
                if (expr is ColumnRef col)
                    resultRow[col.Column] = _evaluator.Evaluate(expr, firstRow);
            }

            // Evaluate SELECT items (resolve aggregates)
            foreach (var item in select.Columns)
            {
                if (item is ExpressionSelectItem exprItem)
                {
                    var value = ResolveAggregate(exprItem.Expression, group.ToList());
                    var colName = exprItem.Alias ?? GetExpressionName(exprItem.Expression);
                    resultRow[colName] = value;
                }
            }

            return resultRow;
        });
    }

    private object? ResolveAggregate(SqlExpression expr, List<Dictionary<string, object?>> groupRows)
    {
        if (expr is FunctionCall func)
        {
            var argExpr = func.Arguments.Count > 0 ? func.Arguments[0] : null;
            var values = argExpr != null
                ? groupRows.Select(r => _evaluator.Evaluate(argExpr, r)).ToList()
                : groupRows.Select(_ => (object?)1).ToList();

            return func.Name switch
            {
                "COUNT" => func.Distinct
                    ? values.Where(v => v != null).Distinct().Count()
                    : func.Arguments.Count == 0
                        ? groupRows.Count
                        : values.Count(v => v != null),
                "SUM" => values.Where(v => v != null).Sum(v => SqlExpressionEvaluator.ToDouble(v)),
                "AVG" => values.Where(v => v != null).Average(v => SqlExpressionEvaluator.ToDouble(v)),
                "MIN" => values.Where(v => v != null).MinBy(v => SqlExpressionEvaluator.ToDouble(v)),
                "MAX" => values.Where(v => v != null).MaxBy(v => SqlExpressionEvaluator.ToDouble(v)),
                "COUNT_DISTINCT" => values.Where(v => v != null).Select(v => v?.ToString()).Distinct().Count(),
                "COLLECT_LIST" => values.ToList(),
                "COLLECT_SET" => values.Where(v => v != null).Distinct().ToList(),
                "STDDEV" => CalculateStdDev(values),
                "VARIANCE" => CalculateVariance(values),
                _ => _evaluator.EvaluateFunction(func, groupRows.First())
            };
        }

        // For non-aggregate expressions, just evaluate against first row
        if (expr is ColumnRef)
            return _evaluator.Evaluate(expr, groupRows.First());

        // Binary expressions might contain aggregates
        if (expr is BinaryOp bin)
        {
            var left = ResolveAggregate(bin.Left, groupRows);
            var right = ResolveAggregate(bin.Right, groupRows);
            return bin.Op switch
            {
                BinaryOperator.Plus => SqlExpressionEvaluator.ToDouble(left) + SqlExpressionEvaluator.ToDouble(right),
                BinaryOperator.Minus => SqlExpressionEvaluator.ToDouble(left) - SqlExpressionEvaluator.ToDouble(right),
                BinaryOperator.Multiply => SqlExpressionEvaluator.ToDouble(left) * SqlExpressionEvaluator.ToDouble(right),
                BinaryOperator.Divide => SqlExpressionEvaluator.ToDouble(right) is not 0.0
                    ? SqlExpressionEvaluator.ToDouble(left) / SqlExpressionEvaluator.ToDouble(right)
                    : double.NaN,
                _ => _evaluator.Evaluate(expr, groupRows.First())
            };
        }

        return _evaluator.Evaluate(expr, groupRows.First());
    }

    // === Projection ===

    private IEnumerable<Dictionary<string, object?>> Project(
        IEnumerable<Dictionary<string, object?>> rows, IReadOnlyList<SelectItem> columns)
    {
        // SELECT * — pass through
        if (columns.Count == 1 && columns[0] is StarSelectItem)
            return rows;

        return rows.Select(row =>
        {
            var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in columns)
            {
                switch (item)
                {
                    case StarSelectItem:
                        foreach (var (key, value) in row)
                            result.TryAdd(key, value);
                        break;
                    case ExpressionSelectItem expr:
                        var val = _evaluator.Evaluate(expr.Expression, row);
                        var name = expr.Alias ?? GetExpressionName(expr.Expression);
                        result[name] = val;
                        break;
                }
            }
            return result;
        });
    }

    // === ORDER BY ===

    private IEnumerable<Dictionary<string, object?>> ExecuteOrderBy(
        IEnumerable<Dictionary<string, object?>> rows, IReadOnlyList<OrderByItem> orderBy)
    {
        IOrderedEnumerable<Dictionary<string, object?>>? ordered = null;
        foreach (var item in orderBy)
        {
            if (ordered == null)
            {
                ordered = item.Descending
                    ? rows.OrderByDescending(r => _evaluator.Evaluate(item.Expression, r), ValueComparer.Instance)
                    : rows.OrderBy(r => _evaluator.Evaluate(item.Expression, r), ValueComparer.Instance);
            }
            else
            {
                ordered = item.Descending
                    ? ordered.ThenByDescending(r => _evaluator.Evaluate(item.Expression, r), ValueComparer.Instance)
                    : ordered.ThenBy(r => _evaluator.Evaluate(item.Expression, r), ValueComparer.Instance);
            }
        }
        return ordered ?? rows;
    }

    // === Helpers ===

    private static string GetExpressionName(SqlExpression expr) => expr switch
    {
        ColumnRef col => col.Table != null ? $"{col.Table}.{col.Column}" : col.Column,
        FunctionCall func => $"{func.Name}({string.Join(", ", func.Arguments.Select(GetExpressionName))})",
        LiteralString s => $"'{s.Value}'",
        LiteralNumber n => n.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        BinaryOp bin => $"{GetExpressionName(bin.Left)} {bin.Op} {GetExpressionName(bin.Right)}",
        _ => expr.ToString() ?? "expr"
    };

    private static List<string> ExtractColumnNames(IReadOnlyList<SelectItem> columns)
    {
        var names = new List<string>();
        foreach (var item in columns)
        {
            if (item is StarSelectItem) names.Add("*");
            else if (item is ExpressionSelectItem expr) names.Add(expr.Alias ?? GetExpressionName(expr.Expression));
        }
        return names;
    }

    private static Dictionary<string, object?> ParseJsonRow(string json)
    {
        var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json) ?? [];
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, element) in doc)
        {
            row[key] = element.ValueKind switch
            {
                JsonValueKind.String => element.GetString(),
                JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                _ => element.GetRawText()
            };
        }
        return row;
    }

    private static double? CalculateVariance(List<object?> values)
    {
        var nums = values.Where(v => v != null).Select(v => SqlExpressionEvaluator.ToDouble(v)).ToList();
        if (nums.Count == 0) return null;
        var avg = nums.Average();
        return nums.Average(n => (n - avg) * (n - avg));
    }

    private static double? CalculateStdDev(List<object?> values)
    {
        var variance = CalculateVariance(values);
        return variance.HasValue ? Math.Sqrt(variance.Value) : null;
    }

    /// <summary>
    /// Extract all table/topic names referenced in a SQL query string.
    /// This is a public convenience method for callers who need to discover
    /// which sources a query references before executing it.
    /// </summary>
    public static List<string> ExtractTableNamesFromSql(string sql)
    {
        var statement = SqlParser.Parse(sql);
        return ExtractTableNames(statement);
    }

    /// <summary>
    /// Extract a WindowSpec from the source (if present on a JOIN source).
    /// </summary>
    private static WindowSpec? ExtractWindowSpec(SqlSource source) => source switch
    {
        JoinSource js => js.Window ?? ExtractWindowSpec(js.Left) ?? ExtractWindowSpec(js.Right),
        _ => null
    };

    /// <summary>
    /// Extract all table names referenced in a SQL statement.
    /// Used by ExecuteWithTopicReader to auto-register topic sources.
    /// </summary>
    private static List<string> ExtractTableNames(SqlStatement statement)
    {
        var names = new List<string>();
        switch (statement)
        {
            case SelectStatement select:
                CollectTableNames(select.Source, names);
                break;
            case CreateStreamAsSelect csas:
                CollectTableNames(csas.Query.Source, names);
                break;
            case CreateTableAsSelect ctas:
                CollectTableNames(ctas.Query.Source, names);
                break;
            case CreateMaterializedViewAsSelect mv:
                CollectTableNames(mv.Query.Source, names);
                break;
            case InsertIntoSelect insert:
                CollectTableNames(insert.Query.Source, names);
                break;
        }
        return names;
    }

    private static void CollectTableNames(SqlSource source, List<string> names)
    {
        switch (source)
        {
            case TableSource ts:
                names.Add(ts.Name);
                break;
            case JoinSource js:
                CollectTableNames(js.Left, names);
                CollectTableNames(js.Right, names);
                break;
        }
    }

    private sealed class ValueComparer : IComparer<object?>
    {
        public static readonly ValueComparer Instance = new();
        public int Compare(object? x, object? y) => SqlExpressionEvaluator.CompareValues(x, y);
    }
}

/// <summary>
/// Result of a SQL query execution.
/// </summary>
public sealed record SqlResult(
    List<Dictionary<string, object?>> Rows,
    List<string> ColumnNames,
    bool EmitChanges)
{
    public string? CreatedName { get; init; }
    public bool IsCreateStatement { get; init; }

    /// <summary>
    /// Pretty-print the result as a formatted table.
    /// </summary>
    public string ToFormattedTable(int maxRows = 50)
    {
        var sb = new System.Text.StringBuilder();
        var cols = ColumnNames;
        if (cols.Count == 0 && Rows.Count > 0) cols = [.. Rows[0].Keys];

        // Calculate column widths
        var widths = cols.Select(c => c.Length).ToArray();
        var displayRows = Rows.Take(maxRows).ToList();
        foreach (var row in displayRows)
        {
            for (var i = 0; i < cols.Count; i++)
            {
                row.TryGetValue(cols[i], out var val);
                var len = FormatValue(val).Length;
                if (len > widths[i]) widths[i] = len;
            }
        }

        // Header
        var separator = "+" + string.Join("+", widths.Select(w => new string('-', w + 2))) + "+";
        sb.AppendLine(separator);
        sb.Append('|');
        for (var i = 0; i < cols.Count; i++)
            sb.Append(' ').Append(cols[i].PadRight(widths[i])).Append(" |");
        sb.AppendLine();
        sb.AppendLine(separator);

        // Rows
        foreach (var row in displayRows)
        {
            sb.Append('|');
            for (var i = 0; i < cols.Count; i++)
            {
                row.TryGetValue(cols[i], out var val);
                sb.Append(' ').Append(FormatValue(val).PadRight(widths[i])).Append(" |");
            }
            sb.AppendLine();
        }
        sb.AppendLine(separator);

        if (Rows.Count > maxRows)
            sb.AppendLine($"... and {Rows.Count - maxRows} more rows");

        sb.AppendLine($"{Rows.Count} row(s)");
        return sb.ToString();
    }

    private static string FormatValue(object? val) => val switch
    {
        null => "NULL",
        double d => d.ToString("G", System.Globalization.CultureInfo.InvariantCulture),
        _ => val.ToString() ?? "NULL"
    };
}

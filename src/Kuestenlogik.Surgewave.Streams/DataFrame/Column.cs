using System.Linq.Expressions;

namespace Kuestenlogik.Surgewave.Streams.DataFrame;

/// <summary>
/// Represents a column in a DataFrame, similar to Spark's Column class.
/// </summary>
public class Column
{
    private readonly string _name;
    private readonly Expression? _expression;
    private readonly ColumnType _type;

    internal Column(string name, Expression? expression = null, ColumnType type = ColumnType.Field)
    {
        _name = name;
        _expression = expression;
        _type = type;
    }

    /// <summary>
    /// Creates a column reference by name.
    /// </summary>
    public static Column Col(string name) => new(name);

    /// <summary>
    /// Creates a literal value column.
    /// </summary>
    public static Column Lit<T>(T value) => new($"lit({value})", null, ColumnType.Literal);

    /// <summary>
    /// The column name.
    /// </summary>
    public string Name => _name;

    /// <summary>
    /// The column type.
    /// </summary>
    internal ColumnType Type => _type;

    /// <summary>
    /// Alias the column with a new name.
    /// </summary>
    public Column As(string alias) => new(alias, _expression, _type);

    /// <summary>
    /// Alias using alias() method.
    /// </summary>
    public Column Alias(string alias) => As(alias);

    // Comparison operators
    public Column Eq(Column other) => new($"({_name} == {other._name})", null, ColumnType.Comparison);
    public Column Eq<T>(T value) => new($"({_name} == {value})", null, ColumnType.Comparison);
    public Column NotEq(Column other) => new($"({_name} != {other._name})", null, ColumnType.Comparison);
    public Column NotEq<T>(T value) => new($"({_name} != {value})", null, ColumnType.Comparison);
    public Column Gt(Column other) => new($"({_name} > {other._name})", null, ColumnType.Comparison);
    public Column Gt<T>(T value) => new($"({_name} > {value})", null, ColumnType.Comparison);
    public Column Lt(Column other) => new($"({_name} < {other._name})", null, ColumnType.Comparison);
    public Column Lt<T>(T value) => new($"({_name} < {value})", null, ColumnType.Comparison);
    public Column Gte(Column other) => new($"({_name} >= {other._name})", null, ColumnType.Comparison);
    public Column Gte<T>(T value) => new($"({_name} >= {value})", null, ColumnType.Comparison);
    public Column Lte(Column other) => new($"({_name} <= {other._name})", null, ColumnType.Comparison);
    public Column Lte<T>(T value) => new($"({_name} <= {value})", null, ColumnType.Comparison);

    // Logical operators
    public Column And(Column other) => new($"({_name} AND {other._name})", null, ColumnType.Logical);
    public Column Or(Column other) => new($"({_name} OR {other._name})", null, ColumnType.Logical);
    public Column Not() => new($"NOT({_name})", null, ColumnType.Logical);

    // Null checks
    public Column IsNull() => new($"({_name} IS NULL)", null, ColumnType.Comparison);
    public Column IsNotNull() => new($"({_name} IS NOT NULL)", null, ColumnType.Comparison);

    // String operations
    public Column Contains(string value) => new($"({_name} CONTAINS '{value}')", null, ColumnType.String);
    public Column StartsWith(string value) => new($"({_name} STARTS WITH '{value}')", null, ColumnType.String);
    public Column EndsWith(string value) => new($"({_name} ENDS WITH '{value}')", null, ColumnType.String);
    public Column Like(string pattern) => new($"({_name} LIKE '{pattern}')", null, ColumnType.String);
    public Column Substr(int start, int length) => new($"SUBSTR({_name}, {start}, {length})", null, ColumnType.String);
    public Column Upper() => new($"UPPER({_name})", null, ColumnType.String);
    public Column Lower() => new($"LOWER({_name})", null, ColumnType.String);
    public Column Trim() => new($"TRIM({_name})", null, ColumnType.String);

    // Arithmetic operators
    public static Column operator +(Column left, Column right) => new($"({left._name} + {right._name})", null, ColumnType.Arithmetic);
    public static Column operator -(Column left, Column right) => new($"({left._name} - {right._name})", null, ColumnType.Arithmetic);
    public static Column operator *(Column left, Column right) => new($"({left._name} * {right._name})", null, ColumnType.Arithmetic);
    public static Column operator /(Column left, Column right) => new($"({left._name} / {right._name})", null, ColumnType.Arithmetic);
    public static Column operator %(Column left, Column right) => new($"({left._name} % {right._name})", null, ColumnType.Arithmetic);

    // In/Between
    public Column In<T>(params T[] values) => new($"({_name} IN ({string.Join(", ", values)}))", null, ColumnType.Comparison);
    public Column Between<T>(T lower, T upper) => new($"({_name} BETWEEN {lower} AND {upper})", null, ColumnType.Comparison);

    // Aggregate functions (for grouped DataFrames)
    public static Column Sum(string columnName) => new($"SUM({columnName})", null, ColumnType.Aggregate);
    public static Column Avg(string columnName) => new($"AVG({columnName})", null, ColumnType.Aggregate);
    public static Column Min(string columnName) => new($"MIN({columnName})", null, ColumnType.Aggregate);
    public static Column Max(string columnName) => new($"MAX({columnName})", null, ColumnType.Aggregate);
    public static Column Count(string columnName = "*") => new($"COUNT({columnName})", null, ColumnType.Aggregate);
    public static Column CountDistinct(string columnName) => new($"COUNT(DISTINCT {columnName})", null, ColumnType.Aggregate);
    public static Column First(string columnName) => new($"FIRST({columnName})", null, ColumnType.Aggregate);
    public static Column Last(string columnName) => new($"LAST({columnName})", null, ColumnType.Aggregate);
    public static Column CollectList(string columnName) => new($"COLLECT_LIST({columnName})", null, ColumnType.Aggregate);
    public static Column CollectSet(string columnName) => new($"COLLECT_SET({columnName})", null, ColumnType.Aggregate);

    // Window functions
    public Column Over(WindowSpec window) => new($"{_name} OVER ({window})", null, ColumnType.Window);

    public override string ToString() => _name;
}

/// <summary>
/// Type of column operation.
/// </summary>
internal enum ColumnType
{
    Field,
    Literal,
    Comparison,
    Logical,
    String,
    Arithmetic,
    Aggregate,
    Window
}

/// <summary>
/// Provides column functions similar to Spark's functions object.
/// </summary>
public static class Functions
{
    public static Column Col(string name) => Column.Col(name);
    public static Column Lit<T>(T value) => Column.Lit(value);
    public static Column Sum(string columnName) => Column.Sum(columnName);
    public static Column Avg(string columnName) => Column.Avg(columnName);
    public static Column Min(string columnName) => Column.Min(columnName);
    public static Column Max(string columnName) => Column.Max(columnName);
    public static Column Count(string columnName = "*") => Column.Count(columnName);
    public static Column CountDistinct(string columnName) => Column.CountDistinct(columnName);
    public static Column First(string columnName) => Column.First(columnName);
    public static Column Last(string columnName) => Column.Last(columnName);

    // Date/Time functions
    public static Column CurrentTimestamp() => new Column("CURRENT_TIMESTAMP");
    public static Column CurrentDate() => new Column("CURRENT_DATE");
    public static Column Year(string columnName) => new Column($"YEAR({columnName})");
    public static Column Month(string columnName) => new Column($"MONTH({columnName})");
    public static Column DayOfMonth(string columnName) => new Column($"DAY({columnName})");
    public static Column Hour(string columnName) => new Column($"HOUR({columnName})");
    public static Column Minute(string columnName) => new Column($"MINUTE({columnName})");
    public static Column Second(string columnName) => new Column($"SECOND({columnName})");

    // Math functions
    public static Column Abs(string columnName) => new Column($"ABS({columnName})");
    public static Column Ceil(string columnName) => new Column($"CEIL({columnName})");
    public static Column Floor(string columnName) => new Column($"FLOOR({columnName})");
    public static Column Round(string columnName, int scale = 0) => new Column($"ROUND({columnName}, {scale})");
    public static Column Sqrt(string columnName) => new Column($"SQRT({columnName})");
    public static Column Pow(string columnName, double power) => new Column($"POW({columnName}, {power})");

    // Conditional functions
    public static Column When(Column condition, Column thenValue) => new WhenColumn(condition, thenValue);
    public static Column Coalesce(params Column[] columns) => new Column($"COALESCE({string.Join(", ", columns.Select(c => c.Name))})");
    public static Column If(Column condition, Column trueValue, Column falseValue) =>
        new Column($"IF({condition}, {trueValue}, {falseValue})");

    // Array functions
    public static Column ArrayContains(string columnName, object value) =>
        new Column($"ARRAY_CONTAINS({columnName}, {value})");
    public static Column Size(string columnName) => new Column($"SIZE({columnName})");
    public static Column Explode(string columnName) => new Column($"EXPLODE({columnName})");

    // JSON functions
    public static Column GetJsonObject(string columnName, string path) =>
        new Column($"GET_JSON_OBJECT({columnName}, '{path}')");
    public static Column ToJson(string columnName) => new Column($"TO_JSON({columnName})");
    public static Column FromJson(string columnName, string schema) =>
        new Column($"FROM_JSON({columnName}, '{schema}')");
}

/// <summary>
/// Column for CASE WHEN expressions.
/// </summary>
internal sealed class WhenColumn : Column
{
    private readonly List<(Column Condition, Column Value)> _cases = new();
    private Column? _otherwise;

    public WhenColumn(Column condition, Column value) : base($"CASE WHEN {condition} THEN {value}")
    {
        _cases.Add((condition, value));
    }

    public WhenColumn AddWhen(Column condition, Column value)
    {
        _cases.Add((condition, value));
        return this;
    }

    public Column Otherwise(Column value)
    {
        _otherwise = value;
        return this;
    }
}

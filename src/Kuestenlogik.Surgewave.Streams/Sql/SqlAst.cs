namespace Kuestenlogik.Surgewave.Streams.Sql;

/// <summary>
/// Abstract Syntax Tree nodes for streaming SQL.
/// </summary>

// === Statements ===

internal abstract record SqlStatement;

/// <summary>
/// SELECT columns FROM source [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT n]
/// </summary>
internal sealed record SelectStatement(
    IReadOnlyList<SelectItem> Columns,
    SqlSource Source,
    SqlExpression? Where,
    IReadOnlyList<SqlExpression>? GroupBy,
    SqlExpression? Having,
    IReadOnlyList<OrderByItem>? OrderBy,
    int? Limit,
    bool EmitChanges) : SqlStatement;

/// <summary>
/// CREATE STREAM name AS SELECT ...
/// </summary>
internal sealed record CreateStreamAsSelect(
    string Name,
    SelectStatement Query,
    Dictionary<string, string>? WithProperties) : SqlStatement;

/// <summary>
/// CREATE TABLE name AS SELECT ... (materialized table from aggregation)
/// </summary>
internal sealed record CreateTableAsSelect(
    string Name,
    SelectStatement Query,
    Dictionary<string, string>? WithProperties) : SqlStatement;

/// <summary>
/// CREATE MATERIALIZED VIEW name AS SELECT ...
/// A materialized view tails its source topic in the background and maintains
/// a queryable state store with the aggregated/projected result.
/// </summary>
internal sealed record CreateMaterializedViewAsSelect(
    string Name,
    SelectStatement Query,
    Dictionary<string, string>? WithProperties,
    bool IfNotExists) : SqlStatement;

/// <summary>
/// DROP MATERIALIZED VIEW [IF EXISTS] name
/// </summary>
internal sealed record DropMaterializedView(
    string Name,
    bool IfExists) : SqlStatement;

/// <summary>
/// INSERT INTO target SELECT ...
/// </summary>
internal sealed record InsertIntoSelect(
    string Target,
    SelectStatement Query) : SqlStatement;

// === Sources ===

internal abstract record SqlSource;

internal sealed record TableSource(string Name, string? Alias) : SqlSource;

internal sealed record JoinSource(
    SqlSource Left,
    SqlSource Right,
    JoinType JoinType,
    SqlExpression On,
    WindowSpec? Window) : SqlSource;

internal enum JoinType { Inner, Left, Right, Full }

// === SELECT items ===

internal abstract record SelectItem;
internal sealed record StarSelectItem() : SelectItem;
internal sealed record ExpressionSelectItem(SqlExpression Expression, string? Alias) : SelectItem;

// === Expressions ===

internal abstract record SqlExpression;

internal sealed record ColumnRef(string? Table, string Column) : SqlExpression;
internal sealed record LiteralString(string Value) : SqlExpression;
internal sealed record LiteralNumber(double Value) : SqlExpression;
internal sealed record LiteralBool(bool Value) : SqlExpression;
internal sealed record LiteralNull() : SqlExpression;

internal sealed record BinaryOp(SqlExpression Left, BinaryOperator Op, SqlExpression Right) : SqlExpression;
internal sealed record UnaryOp(UnaryOperator Op, SqlExpression Operand) : SqlExpression;

internal sealed record FunctionCall(string Name, IReadOnlyList<SqlExpression> Arguments, bool Distinct) : SqlExpression;
internal sealed record CastExpression(SqlExpression Expression, string TypeName) : SqlExpression;
internal sealed record CaseExpression(
    SqlExpression? Operand,
    IReadOnlyList<WhenClause> WhenClauses,
    SqlExpression? ElseResult) : SqlExpression;

internal sealed record InExpression(SqlExpression Expression, IReadOnlyList<SqlExpression> Values, bool Negated) : SqlExpression;
internal sealed record BetweenExpression(SqlExpression Expression, SqlExpression Low, SqlExpression High, bool Negated) : SqlExpression;
internal sealed record LikeExpression(SqlExpression Expression, string Pattern, bool Negated) : SqlExpression;
internal sealed record IsNullExpression(SqlExpression Expression, bool Negated) : SqlExpression;

/// <summary>
/// Window specification: TUMBLE(column, INTERVAL '5' MINUTES) or HOP(column, INTERVAL '5' MINUTES, INTERVAL '1' MINUTE)
/// </summary>
internal sealed record WindowSpec(WindowType Type, string TimeColumn, TimeSpan Size, TimeSpan? Advance);
internal enum WindowType { Tumble, Hop, Session }

internal sealed record WhenClause(SqlExpression Condition, SqlExpression Result);

internal enum BinaryOperator
{
    Equals, NotEquals, LessThan, GreaterThan, LessEqual, GreaterEqual,
    And, Or,
    Plus, Minus, Multiply, Divide, Modulo
}

internal enum UnaryOperator { Not, Minus }

// === ORDER BY ===

internal sealed record OrderByItem(SqlExpression Expression, bool Descending);

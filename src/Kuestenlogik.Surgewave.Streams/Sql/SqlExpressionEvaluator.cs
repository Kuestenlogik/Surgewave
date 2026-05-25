using System.Globalization;
using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Streams.Sql;

/// <summary>
/// Evaluates SQL expressions against a row of key-value data.
/// The row is a dictionary of column name → value.
/// </summary>
internal sealed class SqlExpressionEvaluator
{
    /// <summary>
    /// Evaluate an expression against a data row.
    /// </summary>
    public object? Evaluate(SqlExpression expr, IReadOnlyDictionary<string, object?> row)
    {
        return expr switch
        {
            LiteralString s => s.Value,
            LiteralNumber n => n.Value,
            LiteralBool b => b.Value,
            LiteralNull => null,
            ColumnRef col => EvaluateColumn(col, row),
            BinaryOp bin => EvaluateBinary(bin, row),
            UnaryOp unary => EvaluateUnary(unary, row),
            FunctionCall func => EvaluateFunction(func, row),
            CastExpression cast => EvaluateCast(cast, row),
            CaseExpression c => EvaluateCase(c, row),
            InExpression inExpr => EvaluateIn(inExpr, row),
            BetweenExpression between => EvaluateBetween(between, row),
            LikeExpression like => EvaluateLike(like, row),
            IsNullExpression isNull => EvaluateIsNull(isNull, row),
            _ => throw new SqlParseException($"Unsupported expression type: {expr.GetType().Name}")
        };
    }

    /// <summary>
    /// Evaluate a WHERE condition — returns bool.
    /// </summary>
    public bool EvaluateCondition(SqlExpression expr, IReadOnlyDictionary<string, object?> row)
    {
        var result = Evaluate(expr, row);
        return result is true;
    }

    private static object? EvaluateColumn(ColumnRef col, IReadOnlyDictionary<string, object?> row)
    {
        // Try exact match first, then case-insensitive
        if (row.TryGetValue(col.Column, out var val)) return val;

        // Try with table prefix
        if (col.Table != null)
        {
            var qualified = $"{col.Table}.{col.Column}";
            if (row.TryGetValue(qualified, out val)) return val;
        }

        // Case-insensitive fallback
        foreach (var (key, value) in row)
        {
            if (key.Equals(col.Column, StringComparison.OrdinalIgnoreCase))
                return value;
        }

        return null;
    }

    private object? EvaluateBinary(BinaryOp bin, IReadOnlyDictionary<string, object?> row)
    {
        // Short-circuit for AND/OR
        if (bin.Op == BinaryOperator.And)
        {
            var l = Evaluate(bin.Left, row);
            if (l is not true) return false;
            return Evaluate(bin.Right, row) is true;
        }
        if (bin.Op == BinaryOperator.Or)
        {
            var l = Evaluate(bin.Left, row);
            if (l is true) return true;
            return Evaluate(bin.Right, row) is true;
        }

        var left = Evaluate(bin.Left, row);
        var right = Evaluate(bin.Right, row);

        return bin.Op switch
        {
            BinaryOperator.Equals => CompareValues(left, right) == 0,
            BinaryOperator.NotEquals => CompareValues(left, right) != 0,
            BinaryOperator.LessThan => CompareValues(left, right) < 0,
            BinaryOperator.GreaterThan => CompareValues(left, right) > 0,
            BinaryOperator.LessEqual => CompareValues(left, right) <= 0,
            BinaryOperator.GreaterEqual => CompareValues(left, right) >= 0,
            BinaryOperator.Plus => ArithmeticOp(left, right, (a, b) => a + b),
            BinaryOperator.Minus => ArithmeticOp(left, right, (a, b) => a - b),
            BinaryOperator.Multiply => ArithmeticOp(left, right, (a, b) => a * b),
            BinaryOperator.Divide => ArithmeticOp(left, right, (a, b) => b != 0 ? a / b : double.NaN),
            BinaryOperator.Modulo => ArithmeticOp(left, right, (a, b) => b != 0 ? a % b : double.NaN),
            _ => throw new SqlParseException($"Unsupported binary operator: {bin.Op}")
        };
    }

    private object? EvaluateUnary(UnaryOp unary, IReadOnlyDictionary<string, object?> row)
    {
        var val = Evaluate(unary.Operand, row);
        return unary.Op switch
        {
            UnaryOperator.Not => val is not true,
            UnaryOperator.Minus => val != null ? -ToDouble(val) : null,
            _ => throw new SqlParseException($"Unsupported unary operator: {unary.Op}")
        };
    }

    internal object? EvaluateFunction(FunctionCall func, IReadOnlyDictionary<string, object?> row)
    {
        // Scalar functions (evaluated per-row)
        return func.Name switch
        {
            "UPPER" => Evaluate(func.Arguments[0], row)?.ToString()?.ToUpperInvariant(),
            "LOWER" => Evaluate(func.Arguments[0], row)?.ToString()?.ToLowerInvariant(),
            "TRIM" => Evaluate(func.Arguments[0], row)?.ToString()?.Trim(),
            "LTRIM" => Evaluate(func.Arguments[0], row)?.ToString()?.TrimStart(),
            "RTRIM" => Evaluate(func.Arguments[0], row)?.ToString()?.TrimEnd(),
            "LENGTH" or "LEN" => Evaluate(func.Arguments[0], row)?.ToString()?.Length ?? 0,
            "SUBSTRING" or "SUBSTR" => EvaluateSubstring(func, row),
            "CONCAT" => string.Concat(func.Arguments.Select(a => Evaluate(a, row)?.ToString() ?? "")),
            "REPLACE" => EvaluateReplace(func, row),
            "COALESCE" => func.Arguments.Select(a => Evaluate(a, row)).FirstOrDefault(v => v != null),
            "IFNULL" or "NVL" => Evaluate(func.Arguments[0], row) ?? Evaluate(func.Arguments[1], row),
            "ABS" => Math.Abs(ToDouble(Evaluate(func.Arguments[0], row))),
            "ROUND" => func.Arguments.Count > 1
                ? Math.Round(ToDouble(Evaluate(func.Arguments[0], row)), (int)ToDouble(Evaluate(func.Arguments[1], row)))
                : Math.Round(ToDouble(Evaluate(func.Arguments[0], row))),
            "CEIL" or "CEILING" => Math.Ceiling(ToDouble(Evaluate(func.Arguments[0], row))),
            "FLOOR" => Math.Floor(ToDouble(Evaluate(func.Arguments[0], row))),
            "SQRT" => Math.Sqrt(ToDouble(Evaluate(func.Arguments[0], row))),
            "POWER" or "POW" => Math.Pow(
                ToDouble(Evaluate(func.Arguments[0], row)),
                ToDouble(Evaluate(func.Arguments[1], row))),
            "LOG" => Math.Log(ToDouble(Evaluate(func.Arguments[0], row))),
            "LOG10" => Math.Log10(ToDouble(Evaluate(func.Arguments[0], row))),

            // Aggregate functions return the function call itself during per-row evaluation
            // They are resolved at the grouping/aggregation level
            "COUNT" or "SUM" or "AVG" or "MIN" or "MAX"
                or "COUNT_DISTINCT" or "COLLECT_LIST" or "COLLECT_SET"
                or "STDDEV" or "VARIANCE" => func,

            _ => throw new SqlParseException($"Unknown function: {func.Name}")
        };
    }

    private object? EvaluateSubstring(FunctionCall func, IReadOnlyDictionary<string, object?> row)
    {
        var str = Evaluate(func.Arguments[0], row)?.ToString() ?? "";
        var start = (int)ToDouble(Evaluate(func.Arguments[1], row)) - 1; // SQL is 1-based
        if (start < 0) start = 0;
        if (start >= str.Length) return "";
        if (func.Arguments.Count > 2)
        {
            var len = (int)ToDouble(Evaluate(func.Arguments[2], row));
            return str.Substring(start, Math.Min(len, str.Length - start));
        }
        return str[start..];
    }

    private object? EvaluateReplace(FunctionCall func, IReadOnlyDictionary<string, object?> row)
    {
        var str = Evaluate(func.Arguments[0], row)?.ToString() ?? "";
        var old = Evaluate(func.Arguments[1], row)?.ToString() ?? "";
        var rep = Evaluate(func.Arguments[2], row)?.ToString() ?? "";
        return str.Replace(old, rep, StringComparison.Ordinal);
    }

    private object? EvaluateCast(CastExpression cast, IReadOnlyDictionary<string, object?> row)
    {
        var val = Evaluate(cast.Expression, row);
        return cast.TypeName.ToUpperInvariant() switch
        {
            "INT" or "INTEGER" => (int)ToDouble(val),
            "BIGINT" or "LONG" => (long)ToDouble(val),
            "DOUBLE" or "FLOAT" => ToDouble(val),
            "VARCHAR" or "STRING" => val?.ToString(),
            "BOOLEAN" or "BOOL" => val is true or "true" or "TRUE",
            _ => val
        };
    }

    private object? EvaluateCase(CaseExpression c, IReadOnlyDictionary<string, object?> row)
    {
        if (c.Operand != null)
        {
            var operand = Evaluate(c.Operand, row);
            foreach (var when in c.WhenClauses)
            {
                var cond = Evaluate(when.Condition, row);
                if (CompareValues(operand, cond) == 0)
                    return Evaluate(when.Result, row);
            }
        }
        else
        {
            foreach (var when in c.WhenClauses)
            {
                if (EvaluateCondition(when.Condition, row))
                    return Evaluate(when.Result, row);
            }
        }
        return c.ElseResult != null ? Evaluate(c.ElseResult, row) : null;
    }

    private object? EvaluateIn(InExpression inExpr, IReadOnlyDictionary<string, object?> row)
    {
        var val = Evaluate(inExpr.Expression, row);
        var found = inExpr.Values.Any(v => CompareValues(val, Evaluate(v, row)) == 0);
        return inExpr.Negated ? !found : found;
    }

    private object? EvaluateBetween(BetweenExpression between, IReadOnlyDictionary<string, object?> row)
    {
        var val = Evaluate(between.Expression, row);
        var low = Evaluate(between.Low, row);
        var high = Evaluate(between.High, row);
        var inRange = CompareValues(val, low) >= 0 && CompareValues(val, high) <= 0;
        return between.Negated ? !inRange : inRange;
    }

    private object? EvaluateLike(LikeExpression like, IReadOnlyDictionary<string, object?> row)
    {
        var val = Evaluate(like.Expression, row)?.ToString() ?? "";
        var pattern = "^" + Regex.Escape(like.Pattern)
            .Replace("%", ".*")
            .Replace("_", ".") + "$";
        var match = Regex.IsMatch(val, pattern, RegexOptions.IgnoreCase);
        return like.Negated ? !match : match;
    }

    private object? EvaluateIsNull(IsNullExpression isNull, IReadOnlyDictionary<string, object?> row)
    {
        var val = Evaluate(isNull.Expression, row);
        var isNullResult = val == null;
        return isNull.Negated ? !isNullResult : isNullResult;
    }

    // === Comparison & Arithmetic helpers ===

    internal static int CompareValues(object? left, object? right)
    {
        if (left == null && right == null) return 0;
        if (left == null) return -1;
        if (right == null) return 1;

        // Both numeric
        if (IsNumeric(left) && IsNumeric(right))
            return ToDouble(left).CompareTo(ToDouble(right));

        // String comparison
        return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    internal static double ToDouble(object? val) => val switch
    {
        null => 0,
        double d => d,
        int i => i,
        long l => l,
        float f => f,
        decimal m => (double)m,
        string s => double.TryParse(s, CultureInfo.InvariantCulture, out var d) ? d : 0,
        bool b => b ? 1 : 0,
        _ => double.TryParse(val.ToString(), CultureInfo.InvariantCulture, out var d) ? d : 0
    };

    private static bool IsNumeric(object? val) => val is double or int or long or float or decimal;

    private static object? ArithmeticOp(object? left, object? right, Func<double, double, double> op)
    {
        var l = ToDouble(left);
        var r = ToDouble(right);

        // String concatenation for +
        if (left is string && right != null)
            return left.ToString() + right.ToString();

        return op(l, r);
    }
}

using System.Globalization;
using System.Linq.Expressions;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Pipelines.Expressions;

/// <summary>
/// Translates C# predicate lambdas into the broker's Connect condition syntax:
/// a single <c>$.path OP value</c> comparison per condition, where OP is one of
/// <c>==</c>, <c>!=</c>, <c>&gt;</c>, <c>&lt;</c>, <c>&gt;=</c>, <c>&lt;=</c>,
/// <c>contains</c>, <c>startsWith</c>, <c>endsWith</c>. Conjunctions (<c>&amp;&amp;</c>)
/// become multiple conditions that the DSL applies as chained filter nodes.
/// </summary>
internal static class ConditionTranslator
{
    /// <summary>
    /// Translates <paramref name="predicate"/> into one or more broker conditions that must all
    /// hold (logical AND).
    /// </summary>
    public static IReadOnlyList<FilterCondition> Translate(LambdaExpression predicate, JsonNamingPolicy? namingPolicy)
    {
        var conditions = new List<FilterCondition>();
        TranslateInto(predicate.Body, namingPolicy, conditions);
        return conditions;
    }

    /// <summary>
    /// Translates a predicate that must fit a single broker condition (used by nodes that hold
    /// exactly one condition, like If). Throws when the predicate needs <c>&amp;&amp;</c> chaining.
    /// </summary>
    public static FilterCondition TranslateSingle(LambdaExpression predicate, JsonNamingPolicy? namingPolicy)
    {
        var conditions = Translate(predicate, namingPolicy);
        if (conditions.Count != 1)
        {
            throw new ExpressionNotSupportedException(
                "This node evaluates exactly one comparison, but the predicate contains '&&'. " +
                "Move the extra comparisons into a preceding .Filter(...) stage.");
        }

        return conditions[0];
    }

    private static void TranslateInto(Expression expression, JsonNamingPolicy? namingPolicy, List<FilterCondition> conditions)
    {
        switch (expression)
        {
            case BinaryExpression { NodeType: ExpressionType.AndAlso } and:
                TranslateInto(and.Left, namingPolicy, conditions);
                TranslateInto(and.Right, namingPolicy, conditions);
                return;

            case BinaryExpression { NodeType: ExpressionType.OrElse }:
                throw new ExpressionNotSupportedException(
                    "'||' cannot be translated — the broker evaluates one comparison per filter node. " +
                    "Split the predicate into separate pipelines or use the raw condition-string overload.");

            case UnaryExpression { NodeType: ExpressionType.Not } not:
                conditions.Add(TranslateComparison(not.Operand, namingPolicy, negate: true));
                return;

            default:
                conditions.Add(TranslateComparison(expression, namingPolicy, negate: false));
                return;
        }
    }

    private static FilterCondition TranslateComparison(Expression expression, JsonNamingPolicy? namingPolicy, bool negate)
    {
        switch (expression)
        {
            case BinaryExpression binary when TryGetOperator(binary.NodeType) is { } op:
                return BuildBinary(binary, op, namingPolicy, negate);

            case MethodCallExpression call:
                return BuildStringMethod(call, namingPolicy, negate);

            // Nullable presence check: o => o.Priority.HasValue → a null comparison on the
            // property itself. A direct path would address a CLR member that never exists
            // in the payload JSON and silently match nothing.
            case MemberExpression { Member.Name: "HasValue", Expression: { } inner } hasValue
                when Nullable.GetUnderlyingType(inner.Type) is not null && hasValue.Type == typeof(bool):
                var nullablePath = JsonMemberPath.Build(inner, namingPolicy);
                return new FilterCondition($"{nullablePath} {(negate ? "==" : "!=")} null", Negate: false);

            // Bare boolean member: o => o.IsActive / o => !o.IsActive
            case MemberExpression member when member.Type == typeof(bool):
                var path = JsonMemberPath.Build(member, namingPolicy);
                return new FilterCondition($"{path} == {(negate ? "false" : "true")}", Negate: false);

            case UnaryExpression { NodeType: ExpressionType.Not } not:
                return TranslateComparison(not.Operand, namingPolicy, !negate);

            default:
                throw new ExpressionNotSupportedException(
                    $"'{expression}' cannot be translated into the broker's condition syntax. " +
                    "Supported: comparisons of a payload property against a constant (==, !=, >, <, >=, <=), " +
                    "string Contains/StartsWith/EndsWith, bare boolean properties, '!' and '&&'.");
        }
    }

    private static FilterCondition BuildBinary(BinaryExpression binary, string op, JsonNamingPolicy? namingPolicy, bool negate)
    {
        Expression pathSide;
        Expression valueSide;

        if (JsonMemberPath.IsParameterPath(binary.Left))
        {
            pathSide = binary.Left;
            valueSide = binary.Right;
        }
        else if (JsonMemberPath.IsParameterPath(binary.Right))
        {
            pathSide = binary.Right;
            valueSide = binary.Left;
            op = MirrorOperator(op);
        }
        else
        {
            throw new ExpressionNotSupportedException(
                $"'{binary}' does not compare a payload property against a constant. " +
                "One side must be a property path on the lambda parameter.");
        }

        var path = JsonMemberPath.Build(pathSide, namingPolicy);
        var value = EvaluateConstant(valueSide);

        if (value is null && op is not ("==" or "!="))
        {
            throw new ExpressionNotSupportedException(
                $"'{binary}': null only supports == and != comparisons.");
        }

        // C# promotes char operands to int in comparisons, but the payload JSON carries a
        // one-character string — translate the code point back to the character.
        var pathType = Nullable.GetUnderlyingType(UnwrappedType(pathSide)) ?? UnwrappedType(pathSide);
        if (pathType == typeof(char) && value is not null)
        {
            if (op is ">" or "<" or ">=" or "<=")
            {
                throw new ExpressionNotSupportedException(
                    $"'{binary}': the broker compares {op} numerically, but char properties are " +
                    "serialized as strings — relational char comparisons cannot be translated.");
            }

            value = (char)Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        if (op is ">" or "<" or ">=" or "<=" && value is not null && !IsNumeric(value))
        {
            throw new ExpressionNotSupportedException(
                $"'{binary}': the broker evaluates {op} numerically. " +
                "Only numeric values can be compared with relational operators.");
        }

        // The broker answers false whenever the path is missing or the payload is not JSON —
        // BEFORE the operator runs. A direct != would therefore drop records missing the
        // field, where C# null semantics pass them. Emitting == plus the node's negate switch
        // keeps `x != v` and `!(x == v)` equivalent and missing-field-correct. Null stays a
        // direct comparison: for `x != null` a missing field must NOT pass.
        if (op == "!=" && value is not null)
        {
            op = "==";
            negate = !negate;
        }

        return new FilterCondition($"{path} {op} {FormatValue(value)}", negate);
    }

    private static Type UnwrappedType(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            expression = unary.Operand;
        }

        return expression.Type;
    }

    private static FilterCondition BuildStringMethod(MethodCallExpression call, JsonNamingPolicy? namingPolicy, bool negate)
    {
        if (call.Object is null || call.Method.DeclaringType != typeof(string))
        {
            throw new ExpressionNotSupportedException(
                $"'{call}' cannot be translated. Only string Contains/StartsWith/EndsWith on a " +
                "payload property are supported.");
        }

        var op = call.Method.Name switch
        {
            nameof(string.Contains) => "contains",
            nameof(string.StartsWith) => "startsWith",
            nameof(string.EndsWith) => "endsWith",
            _ => throw new ExpressionNotSupportedException(
                $"String method '{call.Method.Name}' cannot be translated. " +
                "Supported: Contains, StartsWith, EndsWith (evaluated case-insensitively by the broker)."),
        };

        if (call.Arguments.Count != 1
            || (call.Arguments[0].Type != typeof(string) && call.Arguments[0].Type != typeof(char)))
        {
            throw new ExpressionNotSupportedException(
                $"'{call}': only the single-argument string/char overloads are supported — the " +
                "broker always compares case-insensitively.");
        }

        var path = JsonMemberPath.Build(call.Object, namingPolicy);
        var value = EvaluateConstant(call.Arguments[0])
            ?? throw new ExpressionNotSupportedException($"'{call}': the argument must not be null.");

        return new FilterCondition($"{path} {op} {FormatValue(value)}", negate);
    }

    private static string? TryGetOperator(ExpressionType nodeType)
    {
        return nodeType switch
        {
            ExpressionType.Equal => "==",
            ExpressionType.NotEqual => "!=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => null,
        };
    }

    private static string MirrorOperator(string op)
    {
        return op switch
        {
            ">" => "<",
            "<" => ">",
            ">=" => "<=",
            "<=" => ">=",
            _ => op,
        };
    }

    private static object? EvaluateConstant(Expression expression)
    {
        if (ContainsParameter(expression))
        {
            throw new ExpressionNotSupportedException(
                $"'{expression}' references the payload on both sides. One side of a comparison " +
                "must be a constant or captured variable.");
        }

        if (expression is ConstantExpression constant)
        {
            return constant.Value;
        }

        try
        {
            return Expression.Lambda(expression).Compile(preferInterpretation: true).DynamicInvoke();
        }
        catch (Exception ex)
        {
            throw new ExpressionNotSupportedException(
                $"'{expression}' could not be evaluated to a constant: {ex.Message}");
        }
    }

    private static bool ContainsParameter(Expression expression)
    {
        var finder = new ParameterFinder();
        finder.Visit(expression);
        return finder.Found;
    }

    private static bool IsNumeric(object value)
    {
        return value is byte or sbyte or short or ushort or int or uint or long or ulong
            or float or double or decimal or Enum;
    }

    private static string FormatValue(object? value)
    {
        switch (value)
        {
            case null:
                return "null";
            case bool b:
                return b ? "true" : "false";
            case Enum e:
                // Matches System.Text.Json's default of serializing enums as their numeric value.
                return Convert.ToInt64(e, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);
            case IFormattable f when IsNumeric(value):
                return f.ToString(null, CultureInfo.InvariantCulture);
            case string s:
                return FormatString(s);
            case char c:
                return FormatString(c.ToString());
            case Guid g:
                return FormatString(g.ToString());
            default:
                throw new ExpressionNotSupportedException(
                    $"Values of type {value.GetType().Name} cannot be compared by the broker. " +
                    "Supported: strings, numbers, booleans, enums, Guids and null.");
        }
    }

    private static string FormatString(string value)
    {
        if (value.Length > 0 && (value[0] is '\'' or '"' || value[^1] is '\'' or '"'))
        {
            throw new ExpressionNotSupportedException(
                $"The string value '{value}' starts or ends with a quote character, which the " +
                "broker's condition parser would strip.");
        }

        if (value.Any(char.IsControl))
        {
            throw new ExpressionNotSupportedException(
                "String values with control characters (newlines, tabs) cannot be expressed in " +
                "the broker's condition syntax.");
        }

        return $"'{value}'";
    }

    private sealed class ParameterFinder : ExpressionVisitor
    {
        public bool Found { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            Found = true;
            return base.VisitParameter(node);
        }
    }
}

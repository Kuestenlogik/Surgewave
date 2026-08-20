using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Pipelines.Expressions;

/// <summary>
/// Extracts a JSON dot-path (<c>$.customer.address.city</c>) from a member-access chain rooted
/// at a lambda parameter. Property names honor <see cref="JsonPropertyNameAttribute"/> first,
/// then the configured naming policy (camelCase by default, matching the platform's
/// <c>JsonSerializerDefaults.Web</c> convention).
/// </summary>
internal static class JsonMemberPath
{
    /// <summary>
    /// Builds the JSON path for <paramref name="expression"/>, which must be a chain of
    /// property/field accesses on the lambda parameter. Boxing conversions and
    /// <c>Nullable&lt;T&gt;.Value</c> accesses are unwrapped.
    /// </summary>
    public static string Build(Expression expression, JsonNamingPolicy? namingPolicy)
    {
        var segments = new Stack<string>();
        var current = Unwrap(expression);

        while (current is MemberExpression member)
        {
            if (IsNullableValueAccess(member))
            {
                current = Unwrap(member.Expression!);
                continue;
            }

            if (IsSystemDeclared(member.Member))
            {
                throw new ExpressionNotSupportedException(
                    $"'{member.Member.DeclaringType!.Name}.{member.Member.Name}' is a CLR member, not a " +
                    "field of the serialized payload — it has no JSON path. Compare the property itself " +
                    "instead (for example o.Priority != null rather than o.Priority.HasValue, or " +
                    "o.Name == \"\" rather than o.Name.Length == 0).");
            }

            segments.Push(ValidateSegment(ConvertName(member.Member, namingPolicy)));
            current = Unwrap(member.Expression ?? throw NotAPath(expression));
        }

        if (current is not ParameterExpression || segments.Count == 0)
        {
            throw NotAPath(expression);
        }

        return "$." + string.Join('.', segments);
    }

    /// <summary>
    /// True when <paramref name="expression"/> is a member chain rooted at a lambda parameter.
    /// </summary>
    public static bool IsParameterPath(Expression expression)
    {
        var current = Unwrap(expression);
        while (current is MemberExpression member)
        {
            current = Unwrap(member.Expression ?? Expression.Constant(null));
        }

        return current is ParameterExpression;
    }

    private static Expression Unwrap(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    private static bool IsNullableValueAccess(MemberExpression member)
    {
        return member.Member.Name == nameof(Nullable<int>.Value)
            && member.Expression is not null
            && Nullable.GetUnderlyingType(member.Expression.Type) is not null;
    }

    private static bool IsSystemDeclared(MemberInfo member)
    {
        var ns = member.DeclaringType?.Namespace;
        return ns is not null && (ns == "System" || ns.StartsWith("System.", StringComparison.Ordinal));
    }

    private static string ValidateSegment(string segment)
    {
        if (segment.Length == 0 || segment.Any(c => c != '_' && !char.IsLetterOrDigit(c)))
        {
            throw new ExpressionNotSupportedException(
                $"The JSON property name '{segment}' contains characters the broker's path syntax " +
                "cannot address ('.' in particular is a path separator there). Rename the property " +
                "or its [JsonPropertyName].");
        }

        return segment;
    }

    private static string ConvertName(MemberInfo member, JsonNamingPolicy? namingPolicy)
    {
        var attribute = member.GetCustomAttribute<JsonPropertyNameAttribute>();
        if (attribute is not null)
        {
            return attribute.Name;
        }

        return namingPolicy?.ConvertName(member.Name) ?? member.Name;
    }

    private static ExpressionNotSupportedException NotAPath(Expression expression)
    {
        return new ExpressionNotSupportedException(
            $"'{expression}' is not a property path on the pipeline's payload type. " +
            "Only chains of property or field accesses on the lambda parameter can be translated " +
            "(for example o => o.Customer.Id).");
    }
}

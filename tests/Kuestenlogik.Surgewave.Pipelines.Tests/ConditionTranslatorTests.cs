using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Pipelines.Expressions;

namespace Kuestenlogik.Surgewave.Pipelines.Tests;

public class ConditionTranslatorTests
{
    private enum OrderKind
    {
        Standard = 0,
        Express = 1,
    }

    private sealed record Customer(string Id, string Name);

    private sealed record Order(
        string Status,
        double Amount,
        bool Active,
        string? Note,
        Customer Customer,
        int? Priority,
        OrderKind Kind,
        char Initial,
        [property: JsonPropertyName("order_ref")] string Reference,
        [property: JsonPropertyName("app.version")] string AppVersion);

    private static IReadOnlyList<FilterCondition> Translate(Expression<Func<Order, bool>> predicate)
        => ConditionTranslator.Translate(predicate, JsonNamingPolicy.CamelCase);

    private static FilterCondition Single(Expression<Func<Order, bool>> predicate)
        => Assert.Single(Translate(predicate));

    [Fact]
    public void NumericComparison_TranslatesToPathAndOperator()
    {
        var condition = Single(o => o.Amount > 1000);

        Assert.Equal("$.amount > 1000", condition.Condition);
        Assert.False(condition.Negate);
    }

    [Theory]
    [InlineData(">=")]
    [InlineData("<=")]
    [InlineData("<")]
    public void RelationalOperators_AreEmitted(string op)
    {
        var condition = op switch
        {
            ">=" => Single(o => o.Amount >= 10),
            "<=" => Single(o => o.Amount <= 10),
            _ => Single(o => o.Amount < 10),
        };

        Assert.Equal($"$.amount {op} 10", condition.Condition);
    }

    [Fact]
    public void StringEquality_QuotesTheValue()
    {
        Assert.Equal("$.status == 'active'", Single(o => o.Status == "active").Condition);
    }

    [Fact]
    public void NotEqual_BecomesNegatedEquality()
    {
        // The broker answers false for a missing field before the operator runs, so a direct
        // != would drop records without the field — == plus negate keeps C# null semantics.
        var condition = Single(o => o.Status != "archived");

        Assert.Equal("$.status == 'archived'", condition.Condition);
        Assert.True(condition.Negate);
    }

    [Fact]
    public void NotEqual_AndNegatedEquality_TranslateIdentically()
    {
        Assert.Equal(Single(o => o.Status != "archived"), Single(o => !(o.Status == "archived")));
    }

    [Fact]
    public void BareBooleanMember_ComparesToTrue()
    {
        Assert.Equal("$.active == true", Single(o => o.Active).Condition);
    }

    [Fact]
    public void NegatedBooleanMember_ComparesToFalse()
    {
        var condition = Single(o => !o.Active);

        Assert.Equal("$.active == false", condition.Condition);
        Assert.False(condition.Negate);
    }

    [Fact]
    public void NullComparisons_UseNullLiteral()
    {
        // null stays a direct comparison: for != null, a missing field must NOT pass.
        var isNull = Single(o => o.Note == null);
        Assert.Equal("$.note == null", isNull.Condition);
        Assert.False(isNull.Negate);

        var notNull = Single(o => o.Note != null);
        Assert.Equal("$.note != null", notNull.Condition);
        Assert.False(notNull.Negate);
    }

    [Fact]
    public void NullableHasValue_BecomesNullComparison()
    {
        var hasValue = Single(o => o.Priority.HasValue);
        Assert.Equal("$.priority != null", hasValue.Condition);
        Assert.False(hasValue.Negate);

        var hasNoValue = Single(o => !o.Priority.HasValue);
        Assert.Equal("$.priority == null", hasNoValue.Condition);
        Assert.False(hasNoValue.Negate);
    }

    [Fact]
    public void CharComparison_UsesTheCharacterNotTheCodePoint()
    {
        // C# promotes char operands to int, but the payload JSON carries a string.
        Assert.Equal("$.initial == 'A'", Single(o => o.Initial == 'A').Condition);

        var notEqual = Single(o => o.Initial != 'B');
        Assert.Equal("$.initial == 'B'", notEqual.Condition);
        Assert.True(notEqual.Negate);
    }

    [Fact]
    public void RelationalCharComparison_Throws()
    {
        Assert.Throws<ExpressionNotSupportedException>(() => Translate(o => o.Initial > 'A'));
    }

    [Fact]
    public void ClrMembers_AreRejectedInsteadOfSilentlyNeverMatching()
    {
        Assert.Throws<ExpressionNotSupportedException>(() => Translate(o => o.Status.Length > 3));
    }

    [Fact]
    public void NestedMemberChain_BuildsDottedPath()
    {
        Assert.Equal("$.customer.name == 'Ada'", Single(o => o.Customer.Name == "Ada").Condition);
    }

    [Fact]
    public void StringMethods_MapToBrokerOperators()
    {
        Assert.Equal("$.status contains 'act'", Single(o => o.Status.Contains("act")).Condition);
        Assert.Equal("$.customer.name startsWith 'A'", Single(o => o.Customer.Name.StartsWith('A')).Condition);
        Assert.Equal("$.status endsWith 'ive'", Single(o => o.Status.EndsWith("ive")).Condition);
    }

    [Fact]
    public void ReversedComparison_MirrorsTheOperator()
    {
        Assert.Equal("$.amount > 1000", Single(o => 1000 < o.Amount).Condition);
        Assert.Equal("$.amount <= 5", Single(o => 5 >= o.Amount).Condition);
    }

    [Fact]
    public void CapturedVariable_IsEvaluated()
    {
        var threshold = 500;
        Assert.Equal("$.amount >= 500", Single(o => o.Amount >= threshold).Condition);
    }

    [Fact]
    public void AndAlso_SplitsIntoMultipleConditions()
    {
        var conditions = Translate(o => o.Amount > 1000 && o.Status == "active" && o.Active);

        Assert.Equal(3, conditions.Count);
        Assert.Equal("$.amount > 1000", conditions[0].Condition);
        Assert.Equal("$.status == 'active'", conditions[1].Condition);
        Assert.Equal("$.active == true", conditions[2].Condition);
    }

    [Fact]
    public void NegatedComparison_SetsNegate()
    {
        var condition = Single(o => !(o.Amount > 10));

        Assert.Equal("$.amount > 10", condition.Condition);
        Assert.True(condition.Negate);
    }

    [Fact]
    public void Enum_UsesNumericValue()
    {
        Assert.Equal("$.kind == 1", Single(o => o.Kind == OrderKind.Express).Condition);
    }

    [Fact]
    public void NullableComparison_TranslatesWithoutValueSegment()
    {
        Assert.Equal("$.priority > 3", Single(o => o.Priority > 3).Condition);
        Assert.Equal("$.priority > 3", Single(o => o.Priority!.Value > 3).Condition);
    }

    [Fact]
    public void JsonPropertyName_OverridesNamingPolicy()
    {
        Assert.Equal("$.order_ref == 'r-1'", Single(o => o.Reference == "r-1").Condition);
    }

    [Fact]
    public void JsonPropertyNameWithDot_Throws()
    {
        // '.' is a path separator in the broker's syntax — a flat "app.version" property
        // would be looked up as nested objects and never match.
        Assert.Throws<ExpressionNotSupportedException>(() => Translate(o => o.AppVersion == "1"));
    }

    [Fact]
    public void NullNamingPolicy_KeepsPascalCase()
    {
        var conditions = ConditionTranslator.Translate(
            (Expression<Func<Order, bool>>)(o => o.Customer.Name == "Ada"), namingPolicy: null);

        Assert.Equal("$.Customer.Name == 'Ada'", Assert.Single(conditions).Condition);
    }

    [Fact]
    public void OrElse_Throws()
    {
        Assert.Throws<ExpressionNotSupportedException>(
            () => Translate(o => o.Amount > 1000 || o.Active));
    }

    [Fact]
    public void Arithmetic_Throws()
    {
        Assert.Throws<ExpressionNotSupportedException>(
            () => Translate(o => o.Amount + 1 > 2));
    }

    [Fact]
    public void UnsupportedMethod_Throws()
    {
        Assert.Throws<ExpressionNotSupportedException>(
            () => Translate(o => o.Status.Substring(1) == "X"));
    }

    [Fact]
    public void RelationalOnString_Throws()
    {
        Assert.Throws<ExpressionNotSupportedException>(
            () => Translate(o => string.Compare(o.Status, "x", StringComparison.Ordinal) > 0));
    }

    [Fact]
    public void QuotedStringValue_Throws()
    {
        Assert.Throws<ExpressionNotSupportedException>(
            () => Translate(o => o.Status == "'quoted'"));
    }

    [Fact]
    public void TranslateSingle_RejectsConjunctions()
    {
        Assert.Throws<ExpressionNotSupportedException>(
            () => ConditionTranslator.TranslateSingle(
                (Expression<Func<Order, bool>>)(o => o.Active && o.Amount > 1), JsonNamingPolicy.CamelCase));
    }
}

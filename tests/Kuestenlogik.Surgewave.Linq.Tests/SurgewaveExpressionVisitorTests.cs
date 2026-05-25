using System.Linq.Expressions;
using Kuestenlogik.Surgewave.Linq;
using Xunit;

namespace Kuestenlogik.Surgewave.Linq.Tests;

public class SurgewaveExpressionVisitorTests
{
    [Fact]
    public void Visit_WhereExpression_ExtractsPredicate()
    {
        var visitor = new SurgewaveExpressionVisitor();
        var source = new List<string>().AsQueryable();
        var expr = source.Where(s => s.Length > 5).Expression;

        visitor.Visit(expr);

        Assert.Single(visitor.WherePredicates);
    }

    [Fact]
    public void Visit_MultipleWhere_ExtractsAll()
    {
        var visitor = new SurgewaveExpressionVisitor();
        var source = new List<string>().AsQueryable();
        var expr = source.Where(s => s.Length > 5).Where(s => s.StartsWith('A')).Expression;

        visitor.Visit(expr);

        Assert.Equal(2, visitor.WherePredicates.Count);
    }

    [Fact]
    public void Visit_TakeExpression_ExtractsCount()
    {
        var visitor = new SurgewaveExpressionVisitor();
        var source = new List<string>().AsQueryable();
        var expr = source.Take(10).Expression;

        visitor.Visit(expr);

        Assert.Equal(10, visitor.TakeCount);
    }

    [Fact]
    public void Visit_SkipExpression_ExtractsCount()
    {
        var visitor = new SurgewaveExpressionVisitor();
        var source = new List<string>().AsQueryable();
        var expr = source.Skip(5).Expression;

        visitor.Visit(expr);

        Assert.Equal(5, visitor.SkipCount);
    }

    [Fact]
    public void Visit_SelectExpression_ExtractsProjection()
    {
        var visitor = new SurgewaveExpressionVisitor();
        var source = new List<string>().AsQueryable();
        var expr = source.Select(s => s.Length).Expression;

        visitor.Visit(expr);

        Assert.NotNull(visitor.SelectProjection);
    }

    [Fact]
    public void Visit_CountExpression_SetsFlag()
    {
        var visitor = new SurgewaveExpressionVisitor();
        var source = new List<string>().AsQueryable();
        var expr = source.Count().GetType();

        // Count() is a terminal — we test via method call expression
        var countExpr = Expression.Call(
            typeof(Queryable), "Count", [typeof(string)],
            source.Expression);

        visitor.Visit(countExpr);

        Assert.True(visitor.IsCount);
    }

    [Fact]
    public void Visit_AnyExpression_SetsFlag()
    {
        var visitor = new SurgewaveExpressionVisitor();
        var source = new List<string>().AsQueryable();

        var anyExpr = Expression.Call(
            typeof(Queryable), "Any", [typeof(string)],
            source.Expression);

        visitor.Visit(anyExpr);

        Assert.True(visitor.IsAny);
    }

    [Fact]
    public void Visit_FirstExpression_SetsFlagAndTake()
    {
        var visitor = new SurgewaveExpressionVisitor();
        var source = new List<string>().AsQueryable();

        var firstExpr = Expression.Call(
            typeof(Queryable), "First", [typeof(string)],
            source.Expression);

        visitor.Visit(firstExpr);

        Assert.True(visitor.IsFirst);
        Assert.Equal(1, visitor.TakeCount);
    }

    [Fact]
    public void Visit_NoOperations_AllDefaults()
    {
        var visitor = new SurgewaveExpressionVisitor();
        var source = new List<string>().AsQueryable();

        visitor.Visit(source.Expression);

        Assert.Empty(visitor.WherePredicates);
        Assert.Null(visitor.SelectProjection);
        Assert.Null(visitor.TakeCount);
        Assert.Null(visitor.SkipCount);
        Assert.False(visitor.IsCount);
        Assert.False(visitor.IsAny);
        Assert.False(visitor.IsFirst);
    }
}

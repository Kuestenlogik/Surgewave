using System.Linq.Expressions;

namespace Kuestenlogik.Surgewave.Linq;

/// <summary>
/// Visits LINQ expression trees and extracts query operations
/// (Where predicates, Select projections, Take/Skip counts, ordering).
/// </summary>
internal sealed class SurgewaveExpressionVisitor : ExpressionVisitor
{
    public List<LambdaExpression> WherePredicates { get; } = [];
    public LambdaExpression? SelectProjection { get; private set; }
    public int? TakeCount { get; private set; }
    public int? SkipCount { get; private set; }
    public bool IsCount { get; private set; }
    public bool IsAny { get; private set; }
    public bool IsFirst { get; private set; }
    public bool IsFirstOrDefault { get; private set; }
    public bool IsSingle { get; private set; }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Process innermost first (visit arguments before handling this node)
        if (node.Arguments.Count > 0)
            Visit(node.Arguments[0]);

        switch (node.Method.Name)
        {
            case "Where" when node.Arguments.Count == 2:
                var whereLambda = ExtractLambda(node.Arguments[1]);
                if (whereLambda != null) WherePredicates.Add(whereLambda);
                break;

            case "Select" when node.Arguments.Count == 2:
                SelectProjection = ExtractLambda(node.Arguments[1]);
                break;

            case "Take" when node.Arguments.Count == 2:
                TakeCount = ExtractInt(node.Arguments[1]);
                break;

            case "Skip" when node.Arguments.Count == 2:
                SkipCount = ExtractInt(node.Arguments[1]);
                break;

            case "Count" or "LongCount":
                IsCount = true;
                if (node.Arguments.Count == 2)
                {
                    var countPredicate = ExtractLambda(node.Arguments[1]);
                    if (countPredicate != null) WherePredicates.Add(countPredicate);
                }
                break;

            case "Any":
                IsAny = true;
                if (node.Arguments.Count == 2)
                {
                    var anyPredicate = ExtractLambda(node.Arguments[1]);
                    if (anyPredicate != null) WherePredicates.Add(anyPredicate);
                }
                break;

            case "First":
                IsFirst = true;
                TakeCount = 1;
                if (node.Arguments.Count == 2)
                {
                    var firstPredicate = ExtractLambda(node.Arguments[1]);
                    if (firstPredicate != null) WherePredicates.Add(firstPredicate);
                }
                break;

            case "FirstOrDefault":
                IsFirstOrDefault = true;
                TakeCount = 1;
                if (node.Arguments.Count == 2)
                {
                    var firstOrDefaultPredicate = ExtractLambda(node.Arguments[1]);
                    if (firstOrDefaultPredicate != null) WherePredicates.Add(firstOrDefaultPredicate);
                }
                break;

            case "Single" or "SingleOrDefault":
                IsSingle = true;
                TakeCount = 2; // Read 2 to detect duplicates
                if (node.Arguments.Count == 2)
                {
                    var singlePredicate = ExtractLambda(node.Arguments[1]);
                    if (singlePredicate != null) WherePredicates.Add(singlePredicate);
                }
                break;
        }

        return node;
    }

    private static LambdaExpression? ExtractLambda(Expression expression)
    {
        if (expression is UnaryExpression { Operand: LambdaExpression lambda })
            return lambda;
        if (expression is LambdaExpression directLambda)
            return directLambda;
        return null;
    }

    private static int? ExtractInt(Expression expression)
    {
        if (expression is ConstantExpression { Value: int value })
            return value;
        return null;
    }
}

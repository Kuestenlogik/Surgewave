namespace Kuestenlogik.Surgewave.Pipelines.Expressions;

/// <summary>
/// Thrown when a C# predicate cannot be translated into the broker's condition syntax.
/// The broker evaluates a single <c>$.path OP value</c> comparison per node
/// (<c>==</c>, <c>!=</c>, <c>&gt;</c>, <c>&lt;</c>, <c>&gt;=</c>, <c>&lt;=</c>,
/// <c>contains</c>, <c>startsWith</c>, <c>endsWith</c>); <c>&amp;&amp;</c> is expressed as
/// chained filter nodes. Anything beyond that — <c>||</c>, arithmetic, method calls other than
/// string Contains/StartsWith/EndsWith — needs the raw condition-string overload instead.
/// </summary>
public sealed class ExpressionNotSupportedException : Exception
{
    public ExpressionNotSupportedException(string message)
        : base(message)
    {
    }
}

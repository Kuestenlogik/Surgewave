namespace Kuestenlogik.Surgewave.Pipelines.Expressions;

/// <summary>
/// One broker-evaluable comparison in the Connect condition syntax
/// (for example <c>$.amount &gt; 1000</c>), optionally negated.
/// A predicate with <c>&amp;&amp;</c> translates to several of these, applied as chained filters.
/// </summary>
/// <param name="Condition">The condition string in <c>$.path OP value</c> syntax.</param>
/// <param name="Negate">Whether the node should invert the condition's result.</param>
public readonly record struct FilterCondition(string Condition, bool Negate);

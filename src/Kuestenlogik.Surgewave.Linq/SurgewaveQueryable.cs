using System.Collections;
using System.Linq.Expressions;

namespace Kuestenlogik.Surgewave.Linq;

/// <summary>
/// IQueryable implementation that represents a LINQ query against a Surgewave topic.
/// Queries are lazily evaluated — nothing is read until enumeration or a terminal operation.
/// </summary>
internal sealed class SurgewaveQueryable<T> : IOrderedQueryable<T>
{
    public SurgewaveQueryable(SurgewaveQueryProvider provider, object source)
    {
        Provider = provider;
        Expression = Expression.Constant(this);
    }

    public SurgewaveQueryable(SurgewaveQueryProvider provider, Expression expression)
    {
        Provider = provider;
        Expression = expression;
    }

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression)!.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

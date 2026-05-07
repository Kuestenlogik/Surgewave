using System.Linq.Expressions;

namespace Kuestenlogik.Surgewave.Linq;

/// <summary>
/// IQueryProvider that translates LINQ expression trees into Surgewave topic scans.
/// </summary>
internal sealed class SurgewaveQueryProvider : IQueryProvider
{
    private readonly object _source;

    public SurgewaveQueryProvider(object source)
    {
        _source = source;
    }

    public IQueryable CreateQuery(Expression expression)
        => throw new NotSupportedException("Non-generic CreateQuery is not supported. Use the generic version.");

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        => new SurgewaveQueryable<TElement>(this, expression);

    public object? Execute(Expression expression)
        => Execute<object>(expression);

    public TResult Execute<TResult>(Expression expression)
    {
        var executor = new SurgewaveQueryExecutor(_source);
        return executor.Execute<TResult>(expression);
    }
}

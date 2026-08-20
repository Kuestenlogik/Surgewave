using System.Linq.Expressions;
using System.Text.Json;
using Kuestenlogik.Surgewave.Pipelines.Expressions;

namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// Typed <see cref="MapBuilder"/> over the payload type <typeparamref name="TSource"/> —
/// source fields are addressed with lambdas instead of raw JSON paths.
/// </summary>
public sealed class MapBuilder<TSource> : MapBuilder
{
    private readonly JsonNamingPolicy? _namingPolicy;

    internal MapBuilder(JsonNamingPolicy? namingPolicy)
    {
        _namingPolicy = namingPolicy;
    }

    /// <summary>
    /// Maps the payload property selected by <paramref name="source"/> to
    /// <paramref name="targetField"/> in the output record.
    /// </summary>
    public MapBuilder<TSource> Field(string targetField, Expression<Func<TSource, object?>> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        Field(targetField, JsonMemberPath.Build(source.Body, _namingPolicy));
        return this;
    }
}

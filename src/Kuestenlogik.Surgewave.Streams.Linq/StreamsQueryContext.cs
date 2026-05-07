using Kuestenlogik.Surgewave.Linq;
using Kuestenlogik.Surgewave.Streams.InteractiveQueries;

namespace Kuestenlogik.Surgewave.Streams.Linq;

/// <summary>
/// Unified query context for both Surgewave topics (via Surgewave.Linq)
/// and Streams state stores.
/// </summary>
public sealed class StreamsQueryContext
{
    private readonly SurgewaveQueryContext _topicContext;
    private readonly IStateStoreRegistry _storeRegistry;

    public StreamsQueryContext(SurgewaveQueryContext topicContext, IStateStoreRegistry storeRegistry)
    {
        _topicContext = topicContext ?? throw new ArgumentNullException(nameof(topicContext));
        _storeRegistry = storeRegistry ?? throw new ArgumentNullException(nameof(storeRegistry));
    }

    /// <summary>
    /// Query a Surgewave topic directly (scans log segments).
    /// </summary>
    public IQueryable<T> Topic<T>(string topic) where T : class
        => _topicContext.Query<T>(topic);

    /// <summary>
    /// Query a materialized state store (reads from in-memory store).
    /// </summary>
    public IQueryable<KeyValue<TKey, TValue>> Store<TKey, TValue>(string storeName)
        => _storeRegistry.QueryStore<TKey, TValue>(storeName);
}

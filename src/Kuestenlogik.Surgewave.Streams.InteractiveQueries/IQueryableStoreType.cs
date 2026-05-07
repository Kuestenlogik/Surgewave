namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Defines an access pattern for a state store, used with Interactive Queries.
/// </summary>
public interface IQueryableStoreType<out TStore>
{
    /// <summary>
    /// Wraps a raw state store into a typed, read-only query interface.
    /// </summary>
    TStore? Create(IStateStore store);

    /// <summary>
    /// Checks whether the given store is accepted by this store type.
    /// </summary>
    bool Accepts(IStateStore store);
}

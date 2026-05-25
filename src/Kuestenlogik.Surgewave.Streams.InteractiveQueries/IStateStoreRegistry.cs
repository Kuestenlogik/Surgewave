namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Registry that tracks all state stores available for Interactive Queries.
/// </summary>
public interface IStateStoreRegistry
{
    /// <summary>Returns metadata for every registered store.</summary>
    IReadOnlyList<StateStoreInfo> GetAllStores();

    /// <summary>Returns the store instance with the given name, or null if not found.</summary>
    /// <param name="name">The store name.</param>
    IStateStore? GetStore(string name);

    /// <summary>Returns metadata for the store with the given name, or null if not found.</summary>
    /// <param name="name">The store name.</param>
    StateStoreInfo? GetStoreInfo(string name);

    /// <summary>Registers a store under the given name.</summary>
    /// <param name="name">The store name.</param>
    /// <param name="store">The store instance.</param>
    void Register(string name, IStateStore store);

    /// <summary>Removes the store registration for the given name.</summary>
    /// <param name="name">The store name.</param>
    void Unregister(string name);
}

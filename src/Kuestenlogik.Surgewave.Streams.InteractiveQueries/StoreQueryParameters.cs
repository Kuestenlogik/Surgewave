namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Parameters for querying a state store by name and type.
/// </summary>
public sealed class StoreQueryParameters<TStore>
{
    public string StoreName { get; }
    public IQueryableStoreType<TStore> StoreType { get; }

    private StoreQueryParameters(string storeName, IQueryableStoreType<TStore> storeType)
    {
        StoreName = storeName;
        StoreType = storeType;
    }

    /// <summary>
    /// Creates query parameters from a store name and store type.
    /// </summary>
    public static StoreQueryParameters<TStore> FromNameAndType(string storeName, IQueryableStoreType<TStore> storeType)
        => new(storeName, storeType);
}

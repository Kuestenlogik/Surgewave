namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Response DTO listing all registered state stores.
/// </summary>
public sealed class StoreListResponse
{
    /// <summary>Metadata for each registered store.</summary>
    public IReadOnlyList<StateStoreInfo> Stores { get; init; } = [];

    /// <summary>Total number of registered stores.</summary>
    public int TotalCount => Stores.Count;
}

/// <summary>
/// Response DTO representing a single store entry.
/// </summary>
public sealed class StoreEntryResponse
{
    /// <summary>The store this entry belongs to.</summary>
    public string StoreName { get; init; } = string.Empty;

    /// <summary>The entry key, serialised to a string.</summary>
    public string Key { get; init; } = string.Empty;

    /// <summary>The entry value, as a JSON-friendly object.</summary>
    public object? Value { get; init; }
}

/// <summary>
/// Response DTO representing a page of store entries.
/// </summary>
public sealed class StoreEntriesResponse
{
    /// <summary>The store these entries belong to.</summary>
    public string StoreName { get; init; } = string.Empty;

    /// <summary>The returned entries for this page.</summary>
    public IReadOnlyList<StoreEntryResponse> Entries { get; init; } = [];

    /// <summary>The zero-based offset used to produce this page.</summary>
    public int Offset { get; init; }

    /// <summary>The maximum number of entries requested per page.</summary>
    public int Limit { get; init; }

    /// <summary>The total approximate entry count in the store.</summary>
    public long TotalCount { get; init; }

    /// <summary>Whether there are more entries beyond this page.</summary>
    public bool HasMore => Offset + Entries.Count < TotalCount;
}

/// <summary>
/// Response DTO for the approximate entry count of a state store.
/// </summary>
public sealed class StoreCountResponse
{
    /// <summary>The store name.</summary>
    public string StoreName { get; init; } = string.Empty;

    /// <summary>The approximate number of entries in the store.</summary>
    public long ApproximateCount { get; init; }
}

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Metadata about a registered state store.
/// </summary>
/// <param name="Name">The unique name of the store.</param>
/// <param name="StoreType">The category of the store (KeyValue, Window, Session, Unknown).</param>
/// <param name="Persistent">Whether the store persists data to disk.</param>
/// <param name="ApproximateEntryCount">An approximate count of entries currently in the store.</param>
public sealed record StateStoreInfo(
    string Name,
    StateStoreType StoreType,
    bool Persistent,
    long ApproximateEntryCount);

/// <summary>
/// The category of a state store.
/// </summary>
public enum StateStoreType
{
    /// <summary>A plain key-value store.</summary>
    KeyValue,

    /// <summary>A time-windowed store.</summary>
    Window,

    /// <summary>A session-windowed store.</summary>
    Session,

    /// <summary>Store type could not be determined.</summary>
    Unknown
}

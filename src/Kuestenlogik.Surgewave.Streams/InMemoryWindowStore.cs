namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// In-memory windowed store — pure cache without any durability. Entries vanish on
/// application restart. Good for tumbling / hopping aggregations where restart semantics
/// fall back to a full re-read of the source topic.
///
/// <para>
/// This class is a thin volatile subclass of <see cref="InMemoryBackedWindowStore{TKey,TValue}"/>.
/// All the Fetch / FetchAll / ExpireOldWindows logic lives in the base class. Use
/// <see cref="PersistentWindowStore{TKey,TValue}"/> if you need the same semantics with a
/// write-ahead log for faster recovery.
/// </para>
/// </summary>
public sealed class InMemoryWindowStore<TKey, TValue> : InMemoryBackedWindowStore<TKey, TValue>
    where TKey : notnull
{
    /// <inheritdoc />
    public override bool Persistent => false;

    /// <summary>
    /// Creates a new in-memory window store. <paramref name="windowSize"/> determines the
    /// Window.EndMs reported for fetched entries; <paramref name="retentionPeriod"/>
    /// controls how long past windows are retained before the retention sweep removes them.
    /// </summary>
    public InMemoryWindowStore(string name, TimeSpan windowSize, TimeSpan retentionPeriod)
        : base(name, windowSize, retentionPeriod)
    {
    }
}

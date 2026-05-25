namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Base interface for all state stores used in stream processing topologies.
/// State stores provide local, fault-tolerant storage for stateful operations
/// such as aggregations, joins, and windowed computations.
/// </summary>
public interface IStateStore : IDisposable
{
    /// <summary>Gets the unique name of this state store.</summary>
    string Name { get; }

    /// <summary>Gets whether this store persists data to disk (true) or is in-memory only (false).</summary>
    bool Persistent { get; }

    /// <summary>Initializes the state store with the processor context.</summary>
    /// <param name="context">The processor context providing access to configuration and metrics.</param>
    void Init(ProcessorContext context);

    /// <summary>Flushes any buffered data to the underlying storage.</summary>
    void Flush();

    /// <summary>Closes the state store and releases associated resources.</summary>
    void Close();
}

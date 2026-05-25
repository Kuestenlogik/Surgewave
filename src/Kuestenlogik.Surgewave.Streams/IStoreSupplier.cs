namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Supplier interface for creating state store instances.
/// Register with <see cref="StreamsBuilder.AddStateStore{TStore}"/> to make stores available to processors.
/// </summary>
/// <typeparam name="TStore">The state store type.</typeparam>
public interface IStoreSupplier<TStore> where TStore : IStateStore
{
    /// <summary>Gets the name of the state store this supplier creates.</summary>
    string Name { get; }

    /// <summary>Creates a new instance of the state store.</summary>
    /// <returns>A new state store instance.</returns>
    TStore Get();
}

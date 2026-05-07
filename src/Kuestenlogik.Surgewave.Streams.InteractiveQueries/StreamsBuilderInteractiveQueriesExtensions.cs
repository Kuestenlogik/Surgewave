using System.Diagnostics.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Opt-in extensions for enabling peer-to-peer state-store queries on a
/// <see cref="StreamsBuilder"/>. Add <c>using Kuestenlogik.Surgewave.Streams.InteractiveQueries;</c>
/// to bring these into scope.
///
/// The core <see cref="StreamsBuilder"/> is intentionally unaware of interactive queries
/// — all the metadata, remote-query and composite-store APIs live here so they can be
/// removed from the dependency graph of Streams consumers that don't need them.
/// </summary>
public static class StreamsBuilderInteractiveQueriesExtensions
{
    /// <summary>
    /// Enables Remote Interactive Queries on this application. Starts a TCP <see cref="RemoteQueryServer"/>
    /// on <paramref name="applicationServer"/> when <see cref="StreamsApplication.Start"/> is called,
    /// tracks peer metadata, and exposes the query APIs used by this extension family.
    /// </summary>
    /// <param name="app">The streams application.</param>
    /// <param name="applicationServer">The host/port this instance advertises to peers.</param>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "Ownership transferred to StreamsApplication which disposes via DisposeAsync.")]
    public static StreamsApplication WithInteractiveQueries(
        this StreamsApplication app,
        HostInfo applicationServer)
    {
        ArgumentNullException.ThrowIfNull(app);
        return app.WithPeerQueries(new PeerQueryProvider(applicationServer));
    }

    /// <summary>
    /// All known Streams application instance metadata. Includes this instance and any registered peers.
    /// Returns an empty collection if no peer-query provider is registered.
    /// </summary>
    public static IReadOnlyCollection<StreamsMetadata> AllMetadata(this StreamsApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);
        return app.PeerQueries?.AllMetadata ?? Array.Empty<StreamsMetadata>();
    }

    /// <summary>
    /// Metadata for all instances that hold the given state store.
    /// </summary>
    public static IReadOnlyCollection<StreamsMetadata> AllMetadataForStore(
        this StreamsApplication app,
        string storeName)
    {
        ArgumentNullException.ThrowIfNull(app);
        return app.PeerQueries?.AllMetadataForStore(storeName) ?? Array.Empty<StreamsMetadata>();
    }

    /// <summary>
    /// Locate the instance that owns the given key in the given state store.
    /// Uses Murmur2 hashing (same as Kafka's default partitioner) for consistent partition placement.
    /// Returns <c>null</c> if no peer-query provider is registered or no metadata is available.
    /// </summary>
    public static StreamsMetadata? MetadataForKey<TKey>(
        this StreamsApplication app,
        string storeName,
        TKey key,
        ISerde<TKey> serde)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(serde);

        if (app.PeerQueries is null) return null;
        var keyBytes = serde.Serialize(key);
        return app.PeerQueries.FindByKey(storeName, keyBytes);
    }

    /// <summary>
    /// Register a peer instance for Remote Interactive Queries. Connects to the peer, fetches its
    /// metadata and adds it to the metadata state. Throws if no peer-query provider is registered.
    /// </summary>
    public static Task RegisterPeerAsync(
        this StreamsApplication app,
        HostInfo peer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(app);
        if (app.PeerQueries is null)
        {
            throw new InvalidOperationException(
                "Call builder.WithInteractiveQueries(hostInfo) before registering peers.");
        }
        return app.PeerQueries.RegisterPeerAsync(peer, cancellationToken);
    }

    /// <summary>
    /// Creates a composite store that transparently routes queries to the correct instance.
    /// For keys owned by this instance, queries the local store directly. For remote keys,
    /// queries the owning instance via TCP.
    /// Throws if no peer-query provider is registered, or if the provider is not the default
    /// <see cref="PeerQueryProvider"/> (composite stores need direct access to the metadata state).
    /// </summary>
    public static CompositeReadOnlyKeyValueStore<TKey, TValue> CreateCompositeStore<TKey, TValue>(
        this StreamsApplication app,
        string storeName,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(app);

        if (app.PeerQueries is not PeerQueryProvider provider)
        {
            throw new InvalidOperationException(
                "Call builder.WithInteractiveQueries(hostInfo) before creating composite stores.");
        }

        return new CompositeReadOnlyKeyValueStore<TKey, TValue>(
            storeName,
            keySerde,
            valueSerde,
            provider.MetadataState,
            app.GetStateStore);
    }

    /// <summary>
    /// Gets a read-only queryable state store using Interactive Queries (IQ v2 style).
    /// Returns <c>default</c> if the store is not found or is not compatible with the
    /// requested store type.
    /// </summary>
    public static TStore? Store<TStore>(
        this StreamsApplication app,
        StoreQueryParameters<TStore> parameters)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(parameters);

        var rawStore = app.GetStateStore(parameters.StoreName);
        if (rawStore == null)
            return default;

        if (!parameters.StoreType.Accepts(rawStore))
            return default;

        return parameters.StoreType.Create(rawStore);
    }

    /// <summary>
    /// Access the underlying <see cref="StreamsMetadataState"/> for advanced scenarios.
    /// Returns <c>null</c> if no peer-query provider is registered.
    /// Prefer <see cref="AllMetadata"/> / <see cref="AllMetadataForStore"/> / <see cref="MetadataForKey{TKey}"/>.
    /// </summary>
    public static StreamsMetadataState? MetadataState(this StreamsApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);
        return (app.PeerQueries as PeerQueryProvider)?.MetadataState;
    }
}

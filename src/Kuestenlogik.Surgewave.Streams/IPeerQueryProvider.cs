using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Abstraction for peer-to-peer state store query infrastructure.
/// A <see cref="StreamsBuilder"/> delegates all metadata / remote-query operations to this provider
/// when one is registered via <c>WithPeerQueries</c> / <c>WithInteractiveQueries</c>.
/// The default implementation ships in <c>Kuestenlogik.Surgewave.Streams.InteractiveQueries</c>.
/// </summary>
public interface IPeerQueryProvider : IAsyncDisposable
{
    /// <summary>
    /// The host this Streams instance advertises to peers.
    /// </summary>
    HostInfo LocalHost { get; }

    /// <summary>
    /// Starts the provider — opens any listening sockets, begins serving peer queries.
    /// Called once from <see cref="StreamsApplication.Start"/>, after the topology has been assigned.
    /// </summary>
    void Start(PeerQueryContext context);

    /// <summary>
    /// Snapshot of all metadata entries currently known — this instance plus any registered peers.
    /// </summary>
    IReadOnlyCollection<StreamsMetadata> AllMetadata { get; }

    /// <summary>
    /// Metadata entries for all peers that hold the given state store.
    /// </summary>
    IReadOnlyCollection<StreamsMetadata> AllMetadataForStore(string storeName);

    /// <summary>
    /// Locate the peer that owns the given serialized key in the given store.
    /// Returns <c>null</c> if no metadata is available or no partition count is known.
    /// </summary>
    StreamsMetadata? FindByKey(string storeName, byte[] keyBytes);

    /// <summary>
    /// Fetch a peer's metadata over the wire and register it locally.
    /// </summary>
    Task RegisterPeerAsync(HostInfo peer, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces this instance's local metadata after a partition-assignment change.
    /// Called by <see cref="StreamsBuilder"/> from its rebalance listener.
    /// </summary>
    void UpdateLocalMetadata(StreamsMetadata metadata);
}

/// <summary>
/// Runtime context handed to a <see cref="IPeerQueryProvider"/> at startup so it can access
/// live state stores and the current local metadata without having a direct reference to
/// <see cref="StreamsBuilder"/>.
/// </summary>
public sealed record PeerQueryContext(
    Func<string, IStateStore?> StoreResolver,
    Func<StreamsMetadata> GetLocalMetadata,
    ILogger Logger);

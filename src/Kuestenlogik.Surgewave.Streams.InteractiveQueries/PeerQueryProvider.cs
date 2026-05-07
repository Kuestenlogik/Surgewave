using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Default <see cref="IPeerQueryProvider"/> implementation — wires up
/// <see cref="StreamsMetadataState"/> (peer tracking), <see cref="RemoteQueryServer"/>
/// (TCP listener for incoming peer queries) and <see cref="RemoteQueryClient"/>
/// (TCP client for outbound peer queries).
///
/// Register via <c>builder.WithInteractiveQueries(hostInfo)</c>.
/// </summary>
public sealed class PeerQueryProvider : IPeerQueryProvider
{
    private readonly HostInfo _applicationServer;
    private readonly StreamsMetadataState _metadataState;
    private RemoteQueryServer? _queryServer;
    private ILogger? _logger;

    public PeerQueryProvider(HostInfo applicationServer)
    {
        _applicationServer = applicationServer;
        _metadataState = new StreamsMetadataState(applicationServer);
    }

    /// <inheritdoc />
    public HostInfo LocalHost => _applicationServer;

    /// <summary>
    /// The underlying metadata state. Exposed for the extension methods that need to construct
    /// a <see cref="CompositeReadOnlyKeyValueStore{TKey,TValue}"/> — external callers should
    /// use the high-level extension methods on <see cref="StreamsBuilder"/> instead.
    /// </summary>
    public StreamsMetadataState MetadataState => _metadataState;

    public IReadOnlyCollection<StreamsMetadata> AllMetadata => _metadataState.All;

    public IReadOnlyCollection<StreamsMetadata> AllMetadataForStore(string storeName)
        => _metadataState.ForStore(storeName);

    public StreamsMetadata? FindByKey(string storeName, byte[] keyBytes)
    {
        var partitionCount = _metadataState.GetMaxPartitionCount();
        if (partitionCount <= 0) return null;

        var partition = (int)(QueryProtocol.Murmur2(keyBytes) % (uint)partitionCount);
        return _metadataState.FindByPartitionAndStore(partition, storeName);
    }

    public async Task RegisterPeerAsync(HostInfo peer, CancellationToken cancellationToken = default)
    {
        using var client = new RemoteQueryClient(peer);
        var metadata = await client.GetMetadataAsync(cancellationToken);
        if (metadata != null)
        {
            _metadataState.UpdateMetadata(metadata);
            _logger?.LogInformation(
                "Registered peer {Peer} with {Partitions} partitions and {Stores} stores",
                peer, metadata.TopicPartitions.Count, metadata.StateStoreNames.Count);
        }
    }

    public void UpdateLocalMetadata(StreamsMetadata metadata)
        => _metadataState.UpdateMetadata(metadata);

    public void Start(PeerQueryContext context)
    {
        _logger = context.Logger;
        _queryServer = new RemoteQueryServer(
            _applicationServer,
            context.StoreResolver,
            context.GetLocalMetadata,
            _logger);
        _queryServer.Start();
    }

    public async ValueTask DisposeAsync()
    {
        if (_queryServer != null)
        {
            await _queryServer.DisposeAsync();
            _queryServer = null;
        }
    }
}

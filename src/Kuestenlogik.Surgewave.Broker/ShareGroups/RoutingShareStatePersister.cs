using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// Where a partition leader's request about a share partition's state goes (#228): to this broker's own share
/// coordinator when it coordinates the key, else to the broker that does.
/// </summary>
/// <remarks>
/// Without a way to another broker — a broker on its own — every key is the local store's, which answers for the keys it
/// does not coordinate with <see cref="ShareStateStatus.NotCoordinator"/>.
/// </remarks>
public sealed class RoutingShareStatePersister(ShareStateStore local) : IShareStatePersister
{
    private volatile IShareStatePersister? _remote;

    /// <summary>The way to the other brokers' share coordinators, once this broker is in a cluster.</summary>
    public void SetRemote(IShareStatePersister remote) => _remote = remote;

    private IShareStatePersister For(ShareStateKey key) =>
        _remote is { } remote && local.OwnershipOf(key) == CoordinatorOwnership.NotOwned ? remote : local;

    /// <inheritdoc />
    public Task<(ShareStateStatus Status, ShareStateSnapshot? State)> ReadAsync(ShareStateKey key, CancellationToken cancellationToken) =>
        For(key).ReadAsync(key, cancellationToken);

    /// <inheritdoc />
    public Task<ShareStateStatus> WriteAsync(ShareStateKey key, ShareStateSnapshot state, CancellationToken cancellationToken) =>
        For(key).WriteAsync(key, state, cancellationToken);

    /// <inheritdoc />
    public Task<(ShareStateStatus Status, int StateEpoch)> InitializeAsync(ShareStateKey key, long startOffset, CancellationToken cancellationToken) =>
        For(key).InitializeAsync(key, startOffset, cancellationToken);

    /// <inheritdoc />
    public Task<ShareStateStatus> DeleteAsync(ShareStateKey key, CancellationToken cancellationToken) =>
        For(key).DeleteAsync(key, cancellationToken);
}

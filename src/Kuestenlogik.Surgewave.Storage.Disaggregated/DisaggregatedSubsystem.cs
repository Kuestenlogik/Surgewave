using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Read;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;
using Kuestenlogik.Surgewave.Storage.Tiering;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated;

/// <summary>
/// Owns the per-broker disaggregated-storage runtime: the manifest
/// store, the WAL flusher, and the read-side router. The broker DI
/// composes one of these once at startup; LogManager calls
/// <see cref="RegisterPartition"/> when a topic with
/// <c>storage.mode=disaggregated-wal</c> is created or loaded, and
/// <see cref="UnregisterPartition"/> on delete.
///
/// The background flush loop runs while
/// <see cref="StartAsync"/>/<see cref="StopAsync"/> bracket it. Both
/// methods are idempotent — calling StartAsync twice is a no-op,
/// StopAsync without a prior start is also a no-op. The class is
/// safe to be a long-lived singleton.
/// </summary>
public sealed class DisaggregatedSubsystem : IAsyncDisposable
{
    private readonly ConcurrentDictionary<TopicPartition, byte> _watched = new();
    private readonly WalFlusher _flusher;
    private readonly ILogger<DisaggregatedSubsystem> _logger;

    private CancellationTokenSource? _cts;
    private Task? _loop;

    public DisaggregatedSubsystem(
        IPartitionManifestStore manifests,
        IWalSegmentSource segments,
        IRemoteStorageProvider remote,
        WalFlusherOptions? options = null,
        ILogger<DisaggregatedSubsystem>? logger = null,
        ILogger<WalFlusher>? flusherLogger = null)
    {
        Manifests = manifests;
        Remote = remote;
        Reader = new DisaggregatedSegmentReader(manifests, remote);
        _flusher = new WalFlusher(segments, manifests, remote, options, flusherLogger);
        _logger = logger ?? NullLogger<DisaggregatedSubsystem>.Instance;
    }

    /// <summary>The manifest store this subsystem flushes into / reads from.</summary>
    public IPartitionManifestStore Manifests { get; }

    /// <summary>The remote provider underlying both write + read sides.</summary>
    public IRemoteStorageProvider Remote { get; }

    /// <summary>Read-side router for the broker fetch path (P2d/P2e).</summary>
    public DisaggregatedSegmentReader Reader { get; }

    /// <summary>
    /// Mark <paramref name="partition"/> as managed by the flusher. The
    /// next background scan will pick up any sealed segments and start
    /// uploading. Calling twice is a no-op.
    /// </summary>
    public void RegisterPartition(TopicPartition partition) => _watched.TryAdd(partition, 0);

    /// <summary>Stop flushing this partition. Already-uploaded objects stay in the manifest.</summary>
    public void UnregisterPartition(TopicPartition partition) => _watched.TryRemove(partition, out _);

    /// <summary>Snapshot of partitions currently managed by the background loop.</summary>
    public IReadOnlyList<TopicPartition> WatchedPartitions() => _watched.Keys.ToArray();

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_loop is not null) return Task.CompletedTask;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loop = _flusher.RunForeverAsync(WatchedPartitions, _cts.Token);
        _logger.LogInformation("Disaggregated subsystem started — watching {Count} partition(s)", _watched.Count);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_loop is null) return;
        _cts?.Cancel();
        try
        {
            await _loop.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { /* expected on shutdown */ }
        finally
        {
            _cts?.Dispose();
            _cts = null;
            _loop = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        await Remote.DisposeAsync().ConfigureAwait(false);
    }
}

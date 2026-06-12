using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;
using Kuestenlogik.Surgewave.Storage.Tiering;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Stateless;

/// <summary>
/// WarpStream-style write-side agent (ADR-014, P3). Produce batches
/// land in a per-partition RAM buffer; the buffer flushes to the
/// object store on size or age threshold, then commits the new
/// <see cref="StreamObjectRef"/> to the manifest. <c>ProduceAsync</c>
/// awaits the flush — the returned base-offset is durable in S3
/// before the caller is acked. No WAL, no local persistence.
///
/// Single-agent-per-partition assumption in v1: each partition gets
/// one buffer instance owned by this agent process, offsets are
/// assigned sequentially from <c>manifest.LastOffset + 1</c> on first
/// produce. Multi-agent partition sharding is a P-later scaling story
/// (see ADR-014 §"Out of v1").
///
/// Not supported in embedded mode — there is no in-process object
/// store. The broker startup guard in P-final's validator catches
/// that combination before any partition gets registered here.
/// </summary>
public sealed class StatelessAgent : IAsyncDisposable
{
    private readonly IPartitionManifestStore _manifests;
    private readonly IRemoteStorageProvider _remote;
    private readonly StatelessAgentOptions _options;
    private readonly ILogger<StatelessAgent> _logger;
    private readonly ConcurrentDictionary<TopicPartition, PartitionBuffer> _buffers = new();
    private CancellationTokenSource? _cts;
    private Task? _ageLoop;

    public StatelessAgent(
        IPartitionManifestStore manifests,
        IRemoteStorageProvider remote,
        StatelessAgentOptions? options = null,
        ILogger<StatelessAgent>? logger = null)
    {
        _manifests = manifests;
        _remote = remote;
        _options = options ?? new StatelessAgentOptions();
        _logger = logger ?? NullLogger<StatelessAgent>.Instance;
    }

    /// <summary>
    /// Append <paramref name="recordBatch"/> to <paramref name="partition"/>'s
    /// buffer. Returns the assigned <c>baseOffset</c> once the batch has
    /// been durably uploaded to the object store AND the manifest commit
    /// has been accepted. A failed S3 PUT means no offset is returned —
    /// the task faults with <see cref="IOException"/> and the producer
    /// can retry.
    /// </summary>
    public async Task<long> ProduceAsync(
        TopicPartition partition,
        ReadOnlyMemory<byte> recordBatch,
        int recordCount,
        CancellationToken cancellationToken = default)
    {
        var buffer = _buffers.GetOrAdd(partition, _ => new PartitionBuffer(partition));
        var ticket = buffer.Enqueue(recordBatch.ToArray(), recordCount, DateTime.UtcNow);

        if (buffer.PendingBytes >= _options.MaxBufferBytes)
        {
            _ = TriggerFlushAsync(buffer);
        }

        // The ticket completes inside FlushAsync; cancellation gets propagated
        // by linking the caller's CT to the completion source's wait.
        using var registration = cancellationToken.Register(() => ticket.TrySetCanceled(cancellationToken));
        return await ticket.Task.ConfigureAwait(false);
    }

    /// <summary>
    /// Force a flush of <paramref name="partition"/>'s buffer immediately
    /// (test hook + operator-triggered drain on shutdown). Returns the
    /// number of records that were uploaded. No-op when the buffer is
    /// empty.
    /// </summary>
    public Task<int> FlushPartitionAsync(TopicPartition partition, CancellationToken cancellationToken = default)
    {
        if (!_buffers.TryGetValue(partition, out var buffer)) return Task.FromResult(0);
        return TriggerFlushAsync(buffer, cancellationToken);
    }

    public IReadOnlyList<TopicPartition> KnownPartitions() => _buffers.Keys.ToArray();

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_ageLoop is not null) return Task.CompletedTask;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _ageLoop = AgeLoopAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_ageLoop is null) return;
        _cts?.Cancel();
        try { await _ageLoop.WaitAsync(cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) { /* expected */ }
        finally { _cts?.Dispose(); _cts = null; _ageLoop = null; }

        // Drain remaining buffers so a graceful Stop doesn't lose
        // already-acked produces. Cancel-token timeout passes through.
        foreach (var p in KnownPartitions())
        {
            await FlushPartitionAsync(p, cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        await _remote.DisposeAsync().ConfigureAwait(false);
    }

    private async Task<int> TriggerFlushAsync(PartitionBuffer buffer, CancellationToken cancellationToken = default)
    {
        var snapshot = buffer.ExtractForFlush();
        if (snapshot is null) return 0;

        try
        {
            var current = await _manifests.GetAsync(buffer.Partition, cancellationToken).ConfigureAwait(false);
            var baseOffset = (current.LastOffset ?? -1) + 1;
            var lastOffset = baseOffset + snapshot.Value.RecordCount - 1;

            await _remote.UploadSegmentAsync(
                buffer.Partition.Topic,
                buffer.Partition.Partition,
                baseOffset,
                snapshot.Value.LogBytes,
                ReadOnlyMemory<byte>.Empty,
                ReadOnlyMemory<byte>.Empty,
                cancellationToken).ConfigureAwait(false);

            var newRef = new StreamObjectRef(
                ObjectKey: StreamObjectKeyConvention.Build(buffer.Partition, baseOffset),
                FirstOffset: baseOffset,
                LastOffset: lastOffset,
                BytesOnDisk: snapshot.Value.LogBytes.Length,
                CreatedAt: DateTime.UtcNow);
            await _manifests.AppendObjectAsync(buffer.Partition, newRef, cancellationToken).ConfigureAwait(false);

            // Hand every awaiting ProduceAsync its assigned baseOffset (one
            // per enqueued batch — the buffer concatenates batches, the
            // ticket per batch gets the offset of that batch's *first*
            // record so chained reads stay consistent).
            var cursor = baseOffset;
            foreach (var ticket in snapshot.Value.Tickets)
            {
                ticket.Tcs.TrySetResult(cursor);
                cursor += ticket.RecordCount;
            }
            return snapshot.Value.RecordCount;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Stateless flush failed for {Topic}/{Partition}; signalling failure to {Count} producer(s)",
                buffer.Partition.Topic, buffer.Partition.Partition, snapshot.Value.Tickets.Count);
            foreach (var ticket in snapshot.Value.Tickets)
            {
                ticket.Tcs.TrySetException(ex);
            }
            throw;
        }
    }

    private async Task AgeLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.AgePollInterval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { return; }

            foreach (var p in _buffers.Keys)
            {
                if (!_buffers.TryGetValue(p, out var buffer)) continue;
                if (!buffer.IsExpired(_options.MaxBufferAge, DateTime.UtcNow)) continue;
                try
                {
                    await TriggerFlushAsync(buffer, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Age-triggered flush failed for {Topic}/{Partition}",
                        p.Topic, p.Partition);
                }
            }
        }
    }
}

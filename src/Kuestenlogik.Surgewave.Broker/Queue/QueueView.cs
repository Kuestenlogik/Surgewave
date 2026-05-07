using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Queue;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Queue;

/// <summary>
/// Provides RabbitMQ/SQS-style queue semantics on top of Surgewave's immutable log storage.
/// <para>
/// Messages are never removed from the log — replay remains possible.
/// QueueView only tracks which offsets have been delivered, acknowledged, or need re-delivery.
/// </para>
/// <list type="bullet">
///   <item>Deliver → hides message for <see cref="QueueViewConfig.VisibilityTimeout"/>.</item>
///   <item>Ack → advances committed offset, removes from in-flight.</item>
///   <item>Nack with requeue → message immediately eligible for re-delivery.</item>
///   <item>Nack without requeue / <see cref="QueueViewConfig.MaxDeliveryCount"/> exceeded → DLQ.</item>
/// </list>
/// </summary>
public sealed class QueueView : IQueueView
{
    private readonly IPartitionLog _log;
    private readonly LogManager? _logManager;
    private readonly QueueViewConfig _config;
    private readonly ILogger _logger;

    // In-flight messages keyed by MessageId
    private readonly ConcurrentDictionary<string, InFlightMessage> _inFlight = new();

    // Messages waiting for re-delivery (timed-out or nacked-with-requeue)
    private readonly ConcurrentQueue<InFlightMessage> _redeliveryQueue = new();

    // Per-partition committed offsets (highest successfully acked)
    private readonly ConcurrentDictionary<int, long> _committedOffsets = new();

    // Per-partition next read offsets (used for sequential log reads)
    private readonly ConcurrentDictionary<int, long> _readOffsets = new();

    // Background timer for visibility-timeout checking
    private readonly Timer _cleanupTimer;

    // Metrics counters (Interlocked for thread-safe access)
    private long _totalAcked;
    private long _totalNacked;
    private long _totalRejected;
    private long _totalExpired;
    private long _totalRedelivered;
    private long _totalReceived;

    private bool _disposed;

    /// <summary>
    /// Initialises a QueueView for a single topic's partition log.
    /// </summary>
    /// <param name="log">The partition log to read from.</param>
    /// <param name="config">QueueView configuration.</param>
    /// <param name="logger">Logger instance.</param>
    /// <param name="logManager">
    /// Optional LogManager used to auto-create the DLQ topic and write rejected messages.
    /// When null, <see cref="RejectAsync"/> simply removes the message without writing to a DLQ.
    /// </param>
    public QueueView(
        IPartitionLog log,
        QueueViewConfig config,
        ILogger logger,
        LogManager? logManager = null)
    {
        _log = log;
        _config = config;
        _logger = logger;
        _logManager = logManager;

        _cleanupTimer = new Timer(
            OnCleanupTick,
            null,
            config.CleanupInterval,
            config.CleanupInterval);
    }

    /// <summary>Gets the number of messages currently in-flight (delivered, awaiting ack).</summary>
    public int InFlightCount => _inFlight.Count;

    /// <summary>Gets the highest committed (acked) offset for the given partition, or -1 if none.</summary>
    public long CommittedOffset(int partition) =>
        _committedOffsets.TryGetValue(partition, out var v) ? v : -1L;

    /// <inheritdoc/>
    public long TotalAcked => Interlocked.Read(ref _totalAcked);

    /// <inheritdoc/>
    public long TotalNacked => Interlocked.Read(ref _totalNacked);

    /// <inheritdoc/>
    public long TotalRejected => Interlocked.Read(ref _totalRejected);

    /// <inheritdoc/>
    public long TotalExpired => Interlocked.Read(ref _totalExpired);

    /// <inheritdoc/>
    public long TotalRedelivered => Interlocked.Read(ref _totalRedelivered);

    /// <inheritdoc/>
    public long TotalReceived => Interlocked.Read(ref _totalReceived);

    /// <inheritdoc/>
    public IReadOnlyList<IInFlightMessage> GetInFlightMessages() =>
        [.. _inFlight.Values];

    // -------------------------------------------------------------------------
    // Receive
    // -------------------------------------------------------------------------

    /// <summary>
    /// Delivers the next available message(s) from the log to the caller.
    /// Re-delivery queue is checked first; then new messages are read from the log.
    /// Each returned message is placed in-flight with a visibility timeout.
    /// </summary>
    /// <param name="partition">Partition to read from.</param>
    /// <param name="maxMessages">Maximum number of messages to return.</param>
    /// <param name="consumerId">Optional consumer identifier for tracking purposes.</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task<IReadOnlyList<IInFlightMessage>> ReceiveAsync(
        int partition,
        int maxMessages = 1,
        string? consumerId = null,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var result = new List<IInFlightMessage>(maxMessages);

        // 1. Drain re-delivery queue first
        while (result.Count < maxMessages && _redeliveryQueue.TryDequeue(out var redelivered))
        {
            if (redelivered.Partition != partition)
            {
                // Put it back — wrong partition (shouldn't happen in practice but guard anyway)
                _redeliveryQueue.Enqueue(redelivered);
                break;
            }

            redelivered.DeliveryCount++;
            redelivered.ExpiresAt = DateTimeOffset.UtcNow + _config.VisibilityTimeout;
            redelivered.ConsumerId = consumerId;
            _inFlight[redelivered.MessageId] = redelivered;
            Interlocked.Increment(ref _totalRedelivered);
            Interlocked.Increment(ref _totalReceived);
            result.Add(redelivered);
        }

        if (result.Count >= maxMessages)
            return result;

        // 2. Read fresh messages from the log
        var nextOffset = _readOffsets.GetOrAdd(partition, _ =>
        {
            // Start reading from the beginning of what is available
            return _log.LogStartOffset;
        });

        try
        {
            var batches = await _log.ReadBatchesAsync(nextOffset, maxBytes: 1_048_576, ct);
            foreach (var batch in batches)
            {
                if (result.Count >= maxMessages)
                    break;

                var messageId = $"{_log.TopicPartition.Topic}-{partition}-{nextOffset}";

                // Skip if already in-flight
                if (_inFlight.ContainsKey(messageId))
                {
                    nextOffset++;
                    continue;
                }

                var msg = new InFlightMessage
                {
                    MessageId = messageId,
                    Topic = _log.TopicPartition.Topic,
                    Partition = partition,
                    Offset = nextOffset,
                    Body = batch,
                    DeliveryCount = 1,
                    ExpiresAt = DateTimeOffset.UtcNow + _config.VisibilityTimeout,
                    ConsumerId = consumerId
                };

                _inFlight[messageId] = msg;
                _readOffsets[partition] = nextOffset + 1;
                nextOffset++;
                Interlocked.Increment(ref _totalReceived);
                result.Add(msg);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex,
                "QueueView: error reading from partition {Partition} at offset {Offset}",
                partition, nextOffset);
        }

        return result;
    }

    // -------------------------------------------------------------------------
    // Ack
    // -------------------------------------------------------------------------

    /// <summary>
    /// Acknowledges successful processing of a message.
    /// Removes the message from in-flight tracking and advances the committed offset.
    /// </summary>
    /// <param name="messageId">The <see cref="InFlightMessage.MessageId"/> to acknowledge.</param>
    /// <returns><c>true</c> if the message was found and acknowledged; <c>false</c> otherwise.</returns>
    public bool Ack(string messageId)
    {
        if (!_inFlight.TryRemove(messageId, out var msg))
            return false;

        // Advance committed offset (only move forward)
        _committedOffsets.AddOrUpdate(
            msg.Partition,
            msg.Offset,
            (_, existing) => Math.Max(existing, msg.Offset));

        Interlocked.Increment(ref _totalAcked);

        _logger.LogDebug(
            "QueueView Ack: {MessageId} (partition={Partition}, offset={Offset})",
            messageId, msg.Partition, msg.Offset);

        return true;
    }

    // -------------------------------------------------------------------------
    // Nack
    // -------------------------------------------------------------------------

    /// <summary>
    /// Negatively acknowledges a message.
    /// </summary>
    /// <param name="messageId">The <see cref="InFlightMessage.MessageId"/> to nack.</param>
    /// <param name="requeue">
    /// When <c>true</c> (default) the message is placed back in the re-delivery queue immediately.
    /// When <c>false</c> the message is discarded without re-delivery (equivalent to a silent drop).
    /// </param>
    /// <returns><c>true</c> if the message was found; <c>false</c> otherwise.</returns>
    public bool Nack(string messageId, bool requeue = true)
    {
        if (!_inFlight.TryRemove(messageId, out var msg))
            return false;

        Interlocked.Increment(ref _totalNacked);

        if (requeue)
        {
            _redeliveryQueue.Enqueue(msg);
            _logger.LogDebug(
                "QueueView Nack (requeue): {MessageId} delivery={Count}",
                messageId, msg.DeliveryCount);
        }
        else
        {
            _logger.LogDebug(
                "QueueView Nack (drop): {MessageId}",
                messageId);
        }

        return true;
    }

    // -------------------------------------------------------------------------
    // Extend visibility (KIP-932 Renew)
    // -------------------------------------------------------------------------

    /// <inheritdoc />
    public bool ExtendVisibility(string messageId, TimeSpan? extension = null)
    {
        if (!_inFlight.TryGetValue(messageId, out var msg))
        {
            return false;
        }

        var now = DateTimeOffset.UtcNow;
        if (extension is { } extra)
        {
            // Add to whichever is later: the current expiry or now (a lease that already
            // expired must be renewed from "now", not from a stale past timestamp).
            var anchor = msg.ExpiresAt > now ? msg.ExpiresAt : now;
            msg.ExpiresAt = anchor + extra;
        }
        else
        {
            msg.ExpiresAt = now + _config.VisibilityTimeout;
        }

        _logger.LogDebug(
            "QueueView ExtendVisibility: {MessageId} new expiry={Expiry}",
            messageId, msg.ExpiresAt);

        return true;
    }

    // -------------------------------------------------------------------------
    // Reject (→ DLQ)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Rejects a message permanently by routing it to the Dead-Letter-Queue topic and removing it from in-flight.
    /// </summary>
    /// <param name="messageId">The <see cref="InFlightMessage.MessageId"/> to reject.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns><c>true</c> if the message was found and processed; <c>false</c> otherwise.</returns>
    public async Task<bool> RejectAsync(string messageId, CancellationToken ct = default)
    {
        if (!_inFlight.TryRemove(messageId, out var msg))
            return false;

        Interlocked.Increment(ref _totalRejected);

        if (_logManager != null && !string.IsNullOrEmpty(_config.DlqTopicSuffix))
        {
            var dlqTopic = _config.GetDlqTopicName(msg.Topic);
            await EnsureDlqTopicAsync(dlqTopic, ct);

            try
            {
                var dlqTp = new TopicPartition { Topic = dlqTopic, Partition = 0 };
                await _logManager.AppendBatchAsync(dlqTp, msg.Body, ct);

                _logger.LogWarning(
                    "QueueView Reject: {MessageId} routed to DLQ topic '{DlqTopic}' after {Count} deliveries",
                    messageId, dlqTopic, msg.DeliveryCount);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex,
                    "QueueView: failed to write {MessageId} to DLQ topic '{DlqTopic}'",
                    messageId, dlqTopic);
            }
        }
        else
        {
            _logger.LogWarning(
                "QueueView Reject: {MessageId} dropped (no DLQ configured or LogManager unavailable)",
                messageId);
        }

        return true;
    }

    // -------------------------------------------------------------------------
    // Internal: visibility timeout check
    // -------------------------------------------------------------------------

    private void OnCleanupTick(object? state)
    {
        if (_disposed)
            return;

        var now = DateTimeOffset.UtcNow;
        var expired = 0;
        var dlqCandidates = new List<InFlightMessage>();

        // Snapshot expired keys first, then remove — avoids enumerate-while-modify race
        var expiredKeys = new List<string>();
        foreach (var kvp in _inFlight)
        {
            if (kvp.Value.ExpiresAt <= now)
                expiredKeys.Add(kvp.Key);
        }

        foreach (var key in expiredKeys)
        {
            if (_inFlight.TryRemove(key, out var removed))
            {
                expired++;
                Interlocked.Increment(ref _totalExpired);

                if (removed.DeliveryCount >= _config.MaxDeliveryCount)
                {
                    dlqCandidates.Add(removed);
                }
                else
                {
                    _redeliveryQueue.Enqueue(removed);
                    _logger.LogDebug(
                        "QueueView: visibility timeout for {MessageId}, re-queued (delivery {Count}/{Max})",
                        removed.MessageId, removed.DeliveryCount, _config.MaxDeliveryCount);
                }
            }
        }

        if (expired > 0)
        {
            _logger.LogDebug(
                "QueueView cleanup tick: {Expired} messages expired, {DlqCount} sent to DLQ",
                expired, dlqCandidates.Count);
        }

        // DLQ routing for expired max-delivery messages (with error handling)
        if (dlqCandidates.Count > 0)
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    foreach (var msg in dlqCandidates)
                    {
                        if (_logManager != null && !string.IsNullOrEmpty(_config.DlqTopicSuffix))
                        {
                            var dlqTopic = _config.GetDlqTopicName(msg.Topic);
                            await EnsureDlqTopicAsync(dlqTopic, CancellationToken.None);

                            var dlqTp = new TopicPartition { Topic = dlqTopic, Partition = 0 };
                            await _logManager.AppendBatchAsync(dlqTp, msg.Body, CancellationToken.None);
                        }

                        _logger.LogWarning(
                            "QueueView: {MessageId} exceeded MaxDeliveryCount ({Max}), routed to DLQ",
                            msg.MessageId, _config.MaxDeliveryCount);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "QueueView: DLQ routing failed for {Count} messages", dlqCandidates.Count);
                }
            });
        }
    }

    private async Task EnsureDlqTopicAsync(string dlqTopic, CancellationToken ct)
    {
        if (_logManager == null)
            return;

        if (_logManager.GetTopicMetadata(dlqTopic) != null)
            return;

        try
        {
            await _logManager.CreateTopicAsync(
                dlqTopic,
                partitionCount: 1,
                replicationFactor: 1,
                config: new Dictionary<string, string>
                {
                    ["cleanup.policy"] = "delete",
                    ["retention.ms"] = (7 * 24 * 60 * 60 * 1000L).ToString()
                },
                ct);

            _logger.LogInformation("QueueView: auto-created DLQ topic '{DlqTopic}'", dlqTopic);
        }
        catch (InvalidOperationException)
        {
            // Race — already created; that's fine
        }
    }

    // -------------------------------------------------------------------------
    // IAsyncDisposable
    // -------------------------------------------------------------------------

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _cleanupTimer.DisposeAsync();
    }
}

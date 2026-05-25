using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Audit;

/// <summary>
/// Mirrors audit events to a Surgewave topic so a downstream SIEM / compliance
/// pipeline can consume them over the regular Kafka wire (G13 — Confluent
/// Audit Logs parity). The sink is additive: <see cref="AuditLogger"/>
/// continues to write to <c>audit.log</c> as before; this class is only
/// invoked when <see cref="AuditConfig.TopicSinkEnabled"/> is on.
/// </summary>
/// <remarks>
/// Produce path:
/// <list type="number">
///   <item>The audit topic is created on first use (idempotent — repeated
///         <c>CreateTopicAsync</c> calls are swallowed if the topic already
///         exists).</item>
///   <item>Each <see cref="AuditEvent"/> becomes one Kafka record. The key is
///         the principal (or broker id when no principal is known), so
///         per-user audit streams partition cleanly. The value is the
///         JSON-serialised event — same shape as <c>audit.log</c>.</item>
///   <item>The records are framed into a magic-v2 RecordBatch via
///         <see cref="RecordBatchSerializer"/> and appended to partition 0
///         through the standard <see cref="LogManager"/> write pipeline.
///         Partition 0 is intentional — audit events are low volume and the
///         single-partition default keeps ordering trivial; operators who
///         need throughput bump <see cref="AuditConfig.Partitions"/> and the
///         sink hashes by event id.</item>
/// </list>
/// Failures here must never bring down the broker — if a write fails the
/// error is logged and the next batch retries; the file sink remains
/// authoritative.
/// </remarks>
public sealed class AuditTopicSink
{
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _serializer;
    private readonly AuditConfig _config;
    private readonly ILogger<AuditTopicSink> _logger;
    private readonly SemaphoreSlim _topicCreationGuard = new(1, 1);
    private bool _topicReady;

    public AuditTopicSink(
        LogManager logManager,
        RecordBatchSerializer serializer,
        AuditConfig config,
        ILogger<AuditTopicSink> logger)
    {
        _logManager = logManager;
        _serializer = serializer;
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Topic name in use. Useful for tests and diagnostics.
    /// </summary>
    public string TopicName => _config.TopicName;

    /// <summary>
    /// Append a batch of audit events as a single Kafka RecordBatch. Returns
    /// the offset of the last record on success or a negative value if the
    /// write was suppressed by an error (caller should not throw — file sink
    /// already captured the events).
    /// </summary>
    public async ValueTask<long> WriteAsync(IReadOnlyList<AuditEvent> events, CancellationToken cancellationToken = default)
    {
        if (events.Count == 0) return -1;

        try
        {
            await EnsureTopicAsync(cancellationToken).ConfigureAwait(false);

            var messages = new List<Message>(events.Count);
            var baseTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            for (var i = 0; i < events.Count; i++)
            {
                var ev = events[i];
                messages.Add(new Message
                {
                    Offset = i,
                    Timestamp = ev.Timestamp == 0 ? baseTimestamp : ev.Timestamp,
                    Key = SelectKey(ev),
                    Value = JsonSerializer.SerializeToUtf8Bytes(ev),
                    Headers = ReadOnlyMemory<byte>.Empty,
                });
            }

            var batchBytes = _serializer.SerializeMessages(messages);
            var partition = ChoosePartition(events[^1].EventId);
            var topicPartition = new TopicPartition { Topic = _config.TopicName, Partition = partition };

            return await _logManager.AppendBatchAsync(topicPartition, batchBytes, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return -1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to mirror {Count} audit events to topic {Topic} — file sink remains authoritative",
                events.Count, _config.TopicName);
            return -1;
        }
    }

    private async Task EnsureTopicAsync(CancellationToken cancellationToken)
    {
        if (_topicReady) return;
        await _topicCreationGuard.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_topicReady) return;

            try
            {
                var retentionMsString = _config.RetentionMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
                await _logManager.CreateTopicAsync(
                    _config.TopicName,
                    Math.Max(1, _config.Partitions),
                    Math.Max((short)1, _config.ReplicationFactor),
                    new Dictionary<string, string>
                    {
                        ["retention.ms"] = retentionMsString,
                        // Compaction is wrong for an append-only audit log — we
                        // want full history, not last-key-wins. Belt-and-braces
                        // explicit setting in case the broker default flips.
                        ["cleanup.policy"] = "delete",
                    },
                    cancellationToken).ConfigureAwait(false);

                _logger.LogInformation(
                    "Audit topic created: {Topic} (partitions={Partitions}, retentionMs={RetentionMs})",
                    _config.TopicName, _config.Partitions, _config.RetentionMs);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase))
            {
                // Topic survived a previous broker run — that is the normal
                // restart path; nothing to do.
            }

            _topicReady = true;
        }
        finally
        {
            _topicCreationGuard.Release();
        }
    }

    private int ChoosePartition(string eventId)
    {
        var partitions = Math.Max(1, _config.Partitions);
        if (partitions == 1) return 0;
        // Stable hash of EventId (which is GUID-derived). Using the bytes
        // directly keeps allocation off the hot path; a full djb2 / xxHash is
        // overkill for a low-volume audit stream.
        var hash = 0u;
        foreach (var c in eventId)
        {
            hash = (hash * 31) + c;
        }
        return (int)(hash % (uint)partitions);
    }

    private static ReadOnlyMemory<byte> SelectKey(AuditEvent ev)
    {
        var keySource = ev.Principal ?? $"broker-{ev.BrokerId}";
        return Encoding.UTF8.GetBytes(keySource);
    }
}

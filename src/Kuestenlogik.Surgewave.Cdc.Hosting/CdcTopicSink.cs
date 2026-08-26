using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Cdc.Hosting;

/// <summary>
/// Appends captured change events to their topic through the broker's own log
/// (#144) — the sink CDC never had.
/// </summary>
/// <remarks>
/// <para>
/// Written in the shape of <see cref="Audit.AuditTopicSink"/>, which solves the
/// same problem for audit events: create the topic on first use, frame the
/// record into a magic-v2 batch with <see cref="RecordBatchSerializer"/>, and
/// append through the standard <see cref="LogManager"/> pipeline.
/// </para>
/// <para>
/// It differs in one respect, and deliberately. The audit sink swallows write
/// failures because <c>audit.log</c> already holds the event and the next batch
/// can retry. Nothing holds a CDC event: it was read from a replication slot
/// that has already moved on. So a failed append is allowed to propagate, which
/// faults the capture loop for that source and shows up in its status — losing
/// the change silently would recreate the defect this class exists to fix, just
/// one layer down.
/// </para>
/// <para>
/// Partition 0 for now: the key is the row's primary key, so partitioning by it
/// would keep a row's history ordered, but the topics are created with a single
/// partition and hashing across one partition is the identity function. When
/// per-table partition counts become configurable this is where the hash goes.
/// </para>
/// </remarks>
public sealed class CdcTopicSink : ICdcSink
{
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _serializer;
    private readonly ILogger<CdcTopicSink> _logger;
    private readonly SemaphoreSlim _topicCreationGuard = new(1, 1);
    private readonly HashSet<string> _readyTopics = new(StringComparer.Ordinal);

    public CdcTopicSink(
        LogManager logManager,
        RecordBatchSerializer serializer,
        ILogger<CdcTopicSink> logger)
    {
        _logManager = logManager;
        _serializer = serializer;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask WriteAsync(
        string topic,
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        await EnsureTopicAsync(topic, cancellationToken).ConfigureAwait(false);

        var message = new Message
        {
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Key = key ?? ReadOnlyMemory<byte>.Empty,
            Value = value,
            Headers = ReadOnlyMemory<byte>.Empty,
        };

        var batchBytes = _serializer.SerializeMessages([message]);
        var topicPartition = new TopicPartition { Topic = topic, Partition = 0 };

        await _logManager.AppendBatchAsync(topicPartition, batchBytes, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Creates the topic the first time this sink writes to it. CDC discovers
    /// tables at runtime, so unlike the audit sink there is no single topic name
    /// known up front — each one is created when its first change arrives.
    /// </summary>
    private async Task EnsureTopicAsync(string topic, CancellationToken cancellationToken)
    {
        lock (_readyTopics)
        {
            if (_readyTopics.Contains(topic)) return;
        }

        await _topicCreationGuard.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (_readyTopics)
            {
                if (_readyTopics.Contains(topic)) return;
            }

            try
            {
                await _logManager.CreateTopicAsync(
                    topic,
                    partitionCount: 1,
                    replicationFactor: 1,
                    new Dictionary<string, string>
                    {
                        // Compaction, not deletion: a CDC topic is the table's
                        // history, and last-key-wins collapses it to the table's
                        // current state — which is what a consumer rebuilding a
                        // replica wants to be able to do from the start of the
                        // topic without the whole change history being retained
                        // forever.
                        ["cleanup.policy"] = "compact",
                    },
                    cancellationToken).ConfigureAwait(false);

                _logger.LogInformation("CDC topic created: {Topic}", topic);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase))
            {
                // Survived a previous broker run — the normal restart path.
            }

            lock (_readyTopics)
            {
                _readyTopics.Add(topic);
            }
        }
        finally
        {
            _topicCreationGuard.Release();
        }
    }
}

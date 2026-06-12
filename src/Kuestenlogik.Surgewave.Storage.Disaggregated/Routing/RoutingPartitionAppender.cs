using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Stateless;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Routing;

/// <summary>
/// Dispatches <see cref="AppendBatchAsync"/> to the right write path
/// based on the topic's <c>storage.mode</c> (ADR-014). Wraps the
/// classical replicated-path appender (passed in as
/// <c>defaultAppender</c>) and an optional <see cref="StatelessAgent"/>
/// for stateless-mode topics.
///
/// Routing matrix:
/// <list type="table">
///   <item><term><c>replicated</c></term><description>defaultAppender (= LogManager.AppendBatchAsync)</description></item>
///   <item><term><c>disaggregated-wal</c></term><description>defaultAppender — the WAL flusher offloads in the background</description></item>
///   <item><term><c>disaggregated-stateless</c></term><description><see cref="StatelessAgent.ProduceAsync"/></description></item>
/// </list>
///
/// The router asks the host for the topic's mode via the
/// <c>storageModeLookup</c> callback per call — that lets the broker
/// keep its single TopicMetadata cache and avoids duplicating it here.
/// Unknown topics (lookup returns null) fall back to the default
/// appender so a metadata race never strands a Produce.
/// </summary>
public sealed class RoutingPartitionAppender : IPartitionAppender
{
    private readonly IPartitionAppender _defaultAppender;
    private readonly StatelessAgent? _statelessAgent;
    private readonly Func<TopicPartition, StorageMode?> _storageModeLookup;

    public RoutingPartitionAppender(
        IPartitionAppender defaultAppender,
        Func<TopicPartition, StorageMode?> storageModeLookup,
        StatelessAgent? statelessAgent = null)
    {
        _defaultAppender = defaultAppender;
        _storageModeLookup = storageModeLookup;
        _statelessAgent = statelessAgent;
    }

    public Task<long> AppendBatchAsync(
        TopicPartition partition,
        ReadOnlyMemory<byte> recordBatch,
        int recordCount,
        CancellationToken cancellationToken = default)
    {
        var mode = _storageModeLookup(partition);
        if (mode == StorageMode.DisaggregatedStateless)
        {
            if (_statelessAgent is null)
            {
                throw new InvalidOperationException(
                    $"Topic '{partition.Topic}' is in storage.mode='disaggregated-stateless' but no "
                    + "StatelessAgent is registered on this broker. Configure "
                    + "Surgewave:Storage:Disaggregated:Stateless:* or pick storage.mode='replicated'.");
            }
            return _statelessAgent.ProduceAsync(partition, recordBatch, recordCount, cancellationToken);
        }

        // Replicated + disaggregated-wal share the local-append path; the
        // WalFlusher handles offload for the latter outside this hot path.
        return _defaultAppender.AppendBatchAsync(partition, recordBatch, recordCount, cancellationToken);
    }
}

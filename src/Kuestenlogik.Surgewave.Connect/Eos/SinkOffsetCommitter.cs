using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;
using Microsoft.Extensions.Logging;
using CoreTopicPartition = Kuestenlogik.Surgewave.Core.Models.TopicPartition;

namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// Handles transactional offset commits for sink connectors.
/// Provides exactly-once semantics by committing consumer offsets within a transaction.
/// </summary>
public sealed class SinkOffsetCommitter
{
    private readonly SurgewaveNativeClient _nativeClient;
    private readonly string _consumerGroupId;
    private readonly ILogger _logger;

    public SinkOffsetCommitter(
        SurgewaveNativeClient nativeClient,
        string consumerGroupId,
        ILogger logger)
    {
        _nativeClient = nativeClient;
        _consumerGroupId = consumerGroupId;
        _logger = logger;
    }

    /// <summary>
    /// Commits consumer offsets as part of the given transaction.
    /// This ensures exactly-once semantics: offsets are only committed
    /// if the transaction commits successfully.
    /// </summary>
    /// <param name="txnBuilder">The transaction builder to use.</param>
    /// <param name="offsets">Dictionary of TopicPartition to the next offset to consume.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task CommitAsync(
        TransactionBuilder txnBuilder,
        IDictionary<TopicPartition, long> offsets,
        CancellationToken cancellationToken = default)
    {
        if (offsets.Count == 0)
        {
            _logger.LogDebug("No offsets to commit transactionally");
            return;
        }

        _logger.LogDebug(
            "Committing {Count} partition offsets transactionally for group {GroupId}",
            offsets.Count, _consumerGroupId);

        try
        {
            // Convert Connect TopicPartition to Core TopicPartition
            var coreOffsets = new Dictionary<CoreTopicPartition, long>();
            foreach (var (tp, offset) in offsets)
            {
                coreOffsets[new CoreTopicPartition { Topic = tp.Topic, Partition = tp.Partition }] = offset;
            }

            var results = await txnBuilder.SendOffsetsToTransactionAsync(
                _consumerGroupId, coreOffsets, cancellationToken);

            // Check for errors
            foreach (var (topic, partitionResults) in results)
            {
                foreach (var partitionResult in partitionResults)
                {
                    if (partitionResult.ErrorCode != SurgewaveErrorCode.None)
                    {
                        throw new InvalidOperationException(
                            $"Failed to commit offset for {topic}-{partitionResult.Partition}: {partitionResult.ErrorCode}");
                    }
                }
            }

            _logger.LogDebug(
                "Successfully prepared transactional offset commit for group {GroupId}",
                _consumerGroupId);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex,
                "Failed to commit offsets transactionally for group {GroupId}",
                _consumerGroupId);
            throw;
        }
    }

    /// <summary>
    /// Commits consumer offsets from SinkRecords as part of the given transaction.
    /// Calculates the next offset to consume for each partition.
    /// </summary>
    /// <param name="txnBuilder">The transaction builder to use.</param>
    /// <param name="records">The sink records that were processed.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task CommitAsync(
        TransactionBuilder txnBuilder,
        IEnumerable<SinkRecord> records,
        CancellationToken cancellationToken = default)
    {
        // Calculate the highest offset for each partition (plus 1 for next offset)
        var offsets = new Dictionary<TopicPartition, long>();

        foreach (var record in records)
        {
            var tp = new TopicPartition(record.Topic, record.Partition);
            var nextOffset = record.Offset + 1;

            if (!offsets.TryGetValue(tp, out var existingOffset) || nextOffset > existingOffset)
            {
                offsets[tp] = nextOffset;
            }
        }

        return CommitAsync(txnBuilder, offsets, cancellationToken);
    }
}

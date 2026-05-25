using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// Fluent builder for transaction operations.
/// </summary>
public sealed class TransactionBuilder
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _transactionalId;
    private int _timeoutMs = 60000;
    private long _producerId;
    private short _producerEpoch;

    internal TransactionBuilder(SurgewaveNativeClient client, string transactionalId)
    {
        _client = client;
        _transactionalId = transactionalId;
    }

    /// <summary>
    /// Set the transaction timeout.
    /// </summary>
    public TransactionBuilder WithTimeout(TimeSpan timeout)
    {
        _timeoutMs = (int)timeout.TotalMilliseconds;
        return this;
    }

    /// <summary>
    /// Initialize the producer ID and begin the transaction.
    /// </summary>
    public async Task<TransactionBuilder> InitAsync(CancellationToken cancellationToken = default)
    {
        var result = await _client.Transactions.InitProducerIdAsync(_transactionalId, _timeoutMs, cancellationToken);
        if (result.ErrorCode != SurgewaveErrorCode.None)
        {
            throw new InvalidOperationException($"Failed to initialize transaction: {result.ErrorCode}");
        }
        _producerId = result.ProducerId;
        _producerEpoch = result.ProducerEpoch;
        return this;
    }

    /// <summary>
    /// Add partitions to the transaction.
    /// </summary>
    public Task<Dictionary<string, List<PartitionTxnResult>>> AddPartitionsAsync(
        Dictionary<string, List<int>> topics,
        CancellationToken cancellationToken = default)
    {
        return _client.Transactions.AddPartitionsToTxnAsync(_transactionalId, _producerId, _producerEpoch, topics, cancellationToken);
    }

    /// <summary>
    /// Commit the transaction.
    /// </summary>
    public Task<SurgewaveErrorCode> CommitAsync(CancellationToken cancellationToken = default)
    {
        return _client.Transactions.EndTxnAsync(_transactionalId, _producerId, _producerEpoch, commit: true, cancellationToken);
    }

    /// <summary>
    /// Abort the transaction.
    /// </summary>
    public Task<SurgewaveErrorCode> AbortAsync(CancellationToken cancellationToken = default)
    {
        return _client.Transactions.EndTxnAsync(_transactionalId, _producerId, _producerEpoch, commit: false, cancellationToken);
    }

    /// <summary>
    /// Send consumer group offsets to the transaction.
    /// This atomically commits consumer offsets as part of the transaction.
    /// </summary>
    /// <param name="groupId">The consumer group ID.</param>
    /// <param name="offsets">Dictionary of topic to partition offsets.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dictionary of results per topic/partition.</returns>
    public async Task<Dictionary<string, List<PartitionTxnResult>>> SendOffsetsToTransactionAsync(
        string groupId,
        Dictionary<string, List<(int Partition, long Offset, string? Metadata)>> offsets,
        CancellationToken cancellationToken = default)
    {
        // First, add the consumer group to the transaction
        var addResult = await _client.Transactions.AddOffsetsToTxnAsync(
            _transactionalId, _producerId, _producerEpoch, groupId, cancellationToken);

        if (addResult != SurgewaveErrorCode.None)
        {
            throw new InvalidOperationException($"Failed to add offsets to transaction: {addResult}");
        }

        // Convert to TxnOffsetCommit format
        var commitTopics = new Dictionary<string, List<Protocol.Native.Payloads.Transactions.TxnOffsetCommitPartition>>();
        foreach (var (topic, partitionOffsets) in offsets)
        {
            var partitions = new List<Protocol.Native.Payloads.Transactions.TxnOffsetCommitPartition>();
            foreach (var (partition, offset, metadata) in partitionOffsets)
            {
                partitions.Add(new Protocol.Native.Payloads.Transactions.TxnOffsetCommitPartition
                {
                    Partition = partition,
                    CommittedOffset = offset,
                    Metadata = metadata
                });
            }
            commitTopics[topic] = partitions;
        }

        // Commit the offsets within the transaction
        return await _client.Transactions.TxnOffsetCommitAsync(
            _transactionalId, groupId, _producerId, _producerEpoch, commitTopics, cancellationToken);
    }

    /// <summary>
    /// Send consumer group offsets to the transaction using TopicPartition format.
    /// This atomically commits consumer offsets as part of the transaction.
    /// </summary>
    /// <param name="groupId">The consumer group ID.</param>
    /// <param name="offsets">Dictionary of TopicPartition to offset.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<Dictionary<string, List<PartitionTxnResult>>> SendOffsetsToTransactionAsync(
        string groupId,
        IDictionary<TopicPartition, long> offsets,
        CancellationToken cancellationToken = default)
    {
        // Group offsets by topic
        var grouped = new Dictionary<string, List<(int Partition, long Offset, string? Metadata)>>();
        foreach (var (tp, offset) in offsets)
        {
            if (!grouped.TryGetValue(tp.Topic, out var list))
            {
                list = [];
                grouped[tp.Topic] = list;
            }
            list.Add((tp.Partition, offset, null));
        }

        return await SendOffsetsToTransactionAsync(groupId, grouped, cancellationToken);
    }
}

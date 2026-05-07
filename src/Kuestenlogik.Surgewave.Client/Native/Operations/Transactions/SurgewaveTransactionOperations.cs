using Kuestenlogik.Surgewave.Client.Native.Commands;
using Kuestenlogik.Surgewave.Client.Native.Commands.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// Transaction operations for Surgewave native client.
/// </summary>
public sealed class SurgewaveTransactionOperations
{
    private readonly SurgewaveNativeClient _client;
    private readonly CommandExecutor _executor;

    internal SurgewaveTransactionOperations(SurgewaveNativeClient client)
    {
        _client = client;
        _executor = new CommandExecutor(client);
    }

    /// <summary>
    /// Initialize a producer ID for idempotent or transactional producing.
    /// </summary>
    public Task<InitProducerIdResult> InitProducerIdAsync(
        string? transactionalId = null,
        int transactionTimeoutMs = 60000,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new InitProducerIdCommand(transactionalId, transactionTimeoutMs), cancellationToken);

    /// <summary>
    /// Add partitions to an ongoing transaction.
    /// </summary>
    public Task<Dictionary<string, List<PartitionTxnResult>>> AddPartitionsToTxnAsync(
        string transactionalId,
        long producerId,
        short producerEpoch,
        Dictionary<string, List<int>> topics,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new AddPartitionsToTxnCommand(transactionalId, producerId, producerEpoch, topics), cancellationToken);

    /// <summary>
    /// Commit or abort a transaction.
    /// </summary>
    public Task<SurgewaveErrorCode> EndTxnAsync(
        string transactionalId,
        long producerId,
        short producerEpoch,
        bool commit,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new EndTxnCommand(transactionalId, producerId, producerEpoch, commit), cancellationToken);

    /// <summary>
    /// Add consumer group offsets to a transaction.
    /// Must be called before TxnOffsetCommitAsync.
    /// </summary>
    public Task<SurgewaveErrorCode> AddOffsetsToTxnAsync(
        string transactionalId,
        long producerId,
        short producerEpoch,
        string groupId,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new AddOffsetsToTxnCommand(transactionalId, producerId, producerEpoch, groupId), cancellationToken);

    /// <summary>
    /// Commit consumer group offsets within a transaction.
    /// AddOffsetsToTxnAsync must be called before this method.
    /// </summary>
    public Task<Dictionary<string, List<PartitionTxnResult>>> TxnOffsetCommitAsync(
        string transactionalId,
        string groupId,
        long producerId,
        short producerEpoch,
        Dictionary<string, List<Protocol.Native.Payloads.Transactions.TxnOffsetCommitPartition>> topics,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new TxnOffsetCommitCommand(transactionalId, groupId, producerId, producerEpoch, topics), cancellationToken);

    /// <summary>
    /// List all active transactions (for admin/CLI use).
    /// </summary>
    public Task<List<TransactionInfo>> ListAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new ListTransactionsCommand(), cancellationToken);

    /// <summary>
    /// Describe specific transactions (for admin/CLI use).
    /// </summary>
    public Task<List<TransactionDescription>> DescribeAsync(
        List<string> transactionalIds,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new DescribeTransactionsCommand(transactionalIds), cancellationToken);

    /// <summary>
    /// Start building a transaction with fluent API.
    /// </summary>
    public TransactionBuilder BeginTransaction(string transactionalId) => new(_client, transactionalId);
}

using Kuestenlogik.Surgewave.Client.Native.Commands;
using Kuestenlogik.Surgewave.Client.Native.Commands.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// Cross-topic transaction operations for the Surgewave native client.
/// Provides a fluent API for atomic writes across multiple topics.
/// </summary>
public sealed class SurgewaveCrossTopicTransactionOperations : ISurgewaveCrossTopicTransactions
{
    private readonly SurgewaveNativeClient _client;
    private readonly CommandExecutor _executor;

    internal SurgewaveCrossTopicTransactionOperations(SurgewaveNativeClient client)
    {
        _client = client;
        _executor = new CommandExecutor(client);
    }

    /// <summary>
    /// Begin a new cross-topic transaction.
    /// </summary>
    public async Task<SurgewaveCrossTopicTransaction> BeginAsync(TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var timeoutSeconds = timeout.HasValue ? (int)timeout.Value.TotalSeconds : 60;
        var command = new CrossTopicTxnBeginCommand(null, timeoutSeconds);
        var result = await _executor.ExecuteAsync(command, ct);

        if (result.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"Failed to begin cross-topic transaction: {result.ErrorCode}");

        return new SurgewaveCrossTopicTransaction(_client, result.TransactionId);
    }

    /// <summary>
    /// Begin a new cross-topic transaction with a producer ID.
    /// </summary>
    public async Task<SurgewaveCrossTopicTransaction> BeginAsync(string producerId, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var timeoutSeconds = timeout.HasValue ? (int)timeout.Value.TotalSeconds : 60;
        var command = new CrossTopicTxnBeginCommand(producerId, timeoutSeconds);
        var result = await _executor.ExecuteAsync(command, ct);

        if (result.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"Failed to begin cross-topic transaction: {result.ErrorCode}");

        return new SurgewaveCrossTopicTransaction(_client, result.TransactionId);
    }
}

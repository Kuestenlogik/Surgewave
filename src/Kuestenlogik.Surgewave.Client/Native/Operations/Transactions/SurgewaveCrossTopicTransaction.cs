using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native.Commands;
using Kuestenlogik.Surgewave.Client.Native.Commands.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// A cross-topic transaction that buffers writes to multiple topics and commits them atomically.
/// Implements IAsyncDisposable for auto-abort safety net.
///
/// Usage:
/// <code>
/// var tx = await client.CrossTopicTransactions.BeginAsync();
/// tx.Produce("orders", key, orderEvent);
/// tx.Produce("inventory", key, inventoryUpdate);
/// tx.Produce("notifications", key, notification);
/// await tx.CommitAsync();
/// // Or: await tx.AbortAsync();
/// </code>
/// </summary>
public sealed class SurgewaveCrossTopicTransaction : IAsyncDisposable
{
    private readonly SurgewaveNativeClient _client;
    private readonly CommandExecutor _executor;
    private readonly string _transactionId;
    private bool _committed;
    private bool _aborted;
    private bool _disposed;

    internal SurgewaveCrossTopicTransaction(SurgewaveNativeClient client, string transactionId)
    {
        _client = client;
        _executor = new CommandExecutor(client);
        _transactionId = transactionId;
    }

    /// <summary>
    /// The transaction ID assigned by the broker.
    /// </summary>
    public string TransactionId => _transactionId;

    /// <summary>
    /// Add a raw byte message to the transaction.
    /// </summary>
    public SurgewaveCrossTopicTransaction Produce(string topic, byte[]? key, byte[] value, int partition = 0)
    {
        ThrowIfFinalized();

        var command = new CrossTopicTxnAddWriteCommand(_transactionId, topic, partition, key, value);
        var result = _executor.ExecuteAsync(command, CancellationToken.None).GetAwaiter().GetResult();

        if (result.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"Failed to add write to transaction: {result.ErrorCode}");

        return this;
    }

    /// <summary>
    /// Add a typed message to the transaction (JSON serialized).
    /// </summary>
    public SurgewaveCrossTopicTransaction Produce<T>(string topic, string? key, T value, int partition = 0)
    {
        var keyBytes = key != null ? Encoding.UTF8.GetBytes(key) : null;
        var valueBytes = JsonSerializer.SerializeToUtf8Bytes(value);
        return Produce(topic, keyBytes, valueBytes, partition);
    }

    /// <summary>
    /// Add a string message to the transaction.
    /// </summary>
    public SurgewaveCrossTopicTransaction Produce(string topic, string? key, string value, int partition = 0)
    {
        var keyBytes = key != null ? Encoding.UTF8.GetBytes(key) : null;
        var valueBytes = Encoding.UTF8.GetBytes(value);
        return Produce(topic, keyBytes, valueBytes, partition);
    }

    /// <summary>
    /// Add a write asynchronously (for pipeline-friendly usage).
    /// </summary>
    public async Task<SurgewaveCrossTopicTransaction> ProduceAsync(string topic, byte[]? key, byte[] value, int partition = 0, CancellationToken ct = default)
    {
        ThrowIfFinalized();

        var command = new CrossTopicTxnAddWriteCommand(_transactionId, topic, partition, key, value);
        var result = await _executor.ExecuteAsync(command, ct);

        if (result.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"Failed to add write to transaction: {result.ErrorCode}");

        return this;
    }

    /// <summary>
    /// Commit the transaction atomically. All pending writes become visible or none do.
    /// </summary>
    public async Task<CrossTopicTxnCommitResponse> CommitAsync(CancellationToken ct = default)
    {
        ThrowIfFinalized();
        _committed = true;

        var command = new CrossTopicTxnCommitCommand(_transactionId);
        return await _executor.ExecuteAsync(command, ct);
    }

    /// <summary>
    /// Abort the transaction, discarding all pending writes.
    /// </summary>
    public async Task AbortAsync(CancellationToken ct = default)
    {
        ThrowIfFinalized();
        _aborted = true;

        var command = new CrossTopicTxnAbortCommand(_transactionId);
        await _executor.ExecuteAsync(command, ct);
    }

    /// <summary>
    /// Auto-abort if not committed. Safety net for exception paths.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (!_committed && !_aborted)
        {
            try
            {
                await AbortAsync();
            }
            catch
            {
                // Best effort abort on dispose
            }
        }
    }

    private void ThrowIfFinalized()
    {
        if (_committed) throw new InvalidOperationException("Transaction already committed");
        if (_aborted) throw new InvalidOperationException("Transaction already aborted");
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}

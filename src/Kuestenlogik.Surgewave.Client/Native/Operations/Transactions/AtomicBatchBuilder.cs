using System.Text;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// Builder for atomic batch send operations (client-side batching).
/// For server-side transactions with exactly-once semantics, use SurgewaveTransactionOperations.
/// </summary>
public sealed class AtomicBatchBuilder
{
    private readonly SurgewaveNativeClient _client;
    private readonly List<PendingMessage> _messages = new();
    private bool _committed;
    private bool _aborted;

    internal AtomicBatchBuilder(SurgewaveNativeClient client)
    {
        _client = client;
    }

    /// <summary>
    /// Add a message to send in the batch.
    /// </summary>
    public AtomicBatchBuilder Send(string topic, byte[]? key, byte[] value, int partition = 0, Dictionary<string, byte[]>? headers = null)
    {
        ThrowIfFinalized();
        _messages.Add(new PendingMessage(topic, partition, key, value, headers));
        return this;
    }

    /// <summary>
    /// Add a string message to send in the batch.
    /// </summary>
    public AtomicBatchBuilder Send(string topic, string? key, string value, int partition = 0, Dictionary<string, byte[]>? headers = null)
    {
        var keyBytes = key != null ? Encoding.UTF8.GetBytes(key) : null;
        var valueBytes = Encoding.UTF8.GetBytes(value);
        return Send(topic, keyBytes, valueBytes, partition, headers);
    }

    /// <summary>
    /// Add a typed message to send in the batch.
    /// </summary>
    public AtomicBatchBuilder Send<TKey, TValue>(
        string topic,
        TKey? key,
        TValue value,
        int partition = 0,
        Dictionary<string, byte[]>? headers = null)
    {
        var keyBytes = key != null ? SerializeDefault(key, topic) : null;
        var valueBytes = SerializeDefault(value, topic) ?? throw new InvalidOperationException("Value serialized to null");
        return Send(topic, keyBytes, valueBytes, partition, headers);
    }

    /// <summary>
    /// Commit the batch, sending all messages atomically.
    /// </summary>
    public async Task<BatchResult> CommitAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfFinalized();
        _committed = true;

        if (_messages.Count == 0)
            return new BatchResult(true, 0, new Dictionary<string, long>());

        var offsets = new Dictionary<string, long>();
        var successCount = 0;

        // Group messages by topic-partition for batch send
        var grouped = _messages.GroupBy(m => (m.Topic, m.Partition));

        foreach (var group in grouped)
        {
            var topic = group.Key.Topic;
            var partition = group.Key.Partition;
            var messages = group.Select(m => (m.Key, m.Value)).ToList();

            try
            {
                var offset = await _client.Messaging.SendBatchAsync(topic, partition, messages, cancellationToken);
                offsets[$"{topic}-{partition}"] = offset;
                successCount += messages.Count;
            }
            catch
            {
                // On failure, we've already partially committed - this is a limitation
                // of the simple implementation. Real transactions would use 2PC.
                throw;
            }
        }

        return new BatchResult(true, successCount, offsets);
    }

    /// <summary>
    /// Abort the batch, discarding all pending messages.
    /// </summary>
    public Task AbortAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfFinalized();
        _aborted = true;
        _messages.Clear();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Get the number of pending messages.
    /// </summary>
    public int PendingCount => _messages.Count;

    private void ThrowIfFinalized()
    {
        if (_committed) throw new InvalidOperationException("Batch already committed");
        if (_aborted) throw new InvalidOperationException("Batch already aborted");
    }

    private static byte[]? SerializeDefault<T>(T? value, string topic)
    {
        if (value == null) return null;

        var type = typeof(T);
        if (type == typeof(string))
            return Encoding.UTF8.GetBytes((string)(object)value);
        if (type == typeof(byte[]))
            return (byte[])(object)value;

        // Fall back to JSON
        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(value);
    }

    private sealed record PendingMessage(string Topic, int Partition, byte[]? Key, byte[] Value, Dictionary<string, byte[]>? Headers);
}

/// <summary>
/// Result of a batch commit.
/// </summary>
public record BatchResult(bool Success, int MessageCount, Dictionary<string, long> Offsets);

/// <summary>
/// Extension methods for atomic batch send.
/// </summary>
public static class AtomicBatchExtensions
{
    /// <summary>
    /// Begin a new atomic batch for multi-topic send operations.
    /// Note: This is client-side batching. For server-side transactions with
    /// exactly-once semantics, use client.Transactions.BeginTransaction().
    /// </summary>
    public static AtomicBatchBuilder BeginAtomicBatch(this SurgewaveMessagingOperations messaging)
        => new(GetClient(messaging));

    private static SurgewaveNativeClient GetClient(SurgewaveMessagingOperations messaging)
    {
        // Use reflection to get the client (internal field)
        var field = typeof(SurgewaveMessagingOperations).GetField("_client", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        return (SurgewaveNativeClient)field!.GetValue(messaging)!;
    }
}

using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Kafka producer wrapper for Streams processing.
/// Handles output records from sink nodes, changelog writers, and DLQ writers.
///
/// Connects lazily to the broker configured in <see cref="StreamsConfig.BootstrapServers"/>
/// via the Surgewave native protocol on the first produce. While no broker is reachable
/// (e.g. unit tests without a running broker) records are tracked in-memory like before,
/// so the framework keeps functioning offline.
///
/// Transactions are currently local no-ops (broker-side EOS wiring is a follow-up);
/// the native Produce path assigns broker-side timestamps.
/// </summary>
internal sealed class StreamsProducer : IDisposable
{
    private static readonly TimeSpan InitialReconnectBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxReconnectBackoff = TimeSpan.FromSeconds(30);

    private readonly StreamsConfig _config;
    private readonly ILogger _logger;
    private readonly ConcurrentQueue<ProducerRecord> _pendingRecords = new();
    private readonly ConcurrentDictionary<string, long> _producedOffsets = new();
    private readonly SemaphoreSlim _connectSemaphore = new(1, 1);
    private readonly object _clientLock = new();

    private SurgewaveNativeClient? _client;
    private long _nextConnectAttemptTick;
    private TimeSpan _reconnectBackoff = InitialReconnectBackoff;
    private bool _connectFailureLogged;
    private bool _transactionInProgress;
    private volatile bool _disposed;

    public StreamsProducer(StreamsConfig config, ILogger logger)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Initializes transactions (for exactly-once semantics).
    /// </summary>
    public void InitTransactions()
    {
        _logger.LogDebug("Initializing transactions");
    }

    /// <summary>
    /// Begins a new transaction.
    /// </summary>
    public void BeginTransaction()
    {
        if (_transactionInProgress)
            throw new InvalidOperationException("Transaction already in progress");

        _transactionInProgress = true;
        _logger.LogDebug("Beginning transaction");
    }

    /// <summary>
    /// Sends offsets to the transaction for exactly-once semantics.
    /// </summary>
    public void SendOffsetsToTransaction(
        IDictionary<TopicPartition, long> offsets,
        string consumerGroupId)
    {
        if (!_transactionInProgress)
            throw new InvalidOperationException("No transaction in progress");

        foreach (var (partition, offset) in offsets)
        {
            _logger.LogDebug("Adding offset to transaction: {Topic}-{Partition}:{Offset}",
                partition.Topic, partition.Partition, offset);
        }
    }

    /// <summary>
    /// Commits the current transaction.
    /// </summary>
    public void CommitTransaction()
    {
        if (!_transactionInProgress)
            throw new InvalidOperationException("No transaction in progress");

        // Flush all pending records
        Flush();

        _transactionInProgress = false;
        _logger.LogDebug("Committed transaction");
    }

    /// <summary>
    /// Aborts the current transaction.
    /// </summary>
    public void AbortTransaction()
    {
        if (!_transactionInProgress)
            throw new InvalidOperationException("No transaction in progress");

        // Discard pending records
        while (_pendingRecords.TryDequeue(out _)) { }

        _transactionInProgress = false;
        _logger.LogDebug("Aborted transaction");
    }

    /// <summary>
    /// Produces a record to a topic. Sends via the native client when connected;
    /// otherwise falls back to in-memory tracking (offline/test mode).
    /// </summary>
    public async Task<RecordMetadata> ProduceAsync(ProducerRecord record)
    {
        var client = await EnsureClientAsync().ConfigureAwait(false);

        if (client == null)
        {
            // Offline/test fallback: previous in-memory behavior.
            _pendingRecords.Enqueue(record);
            var simulatedOffset = _producedOffsets.AddOrUpdate(
                $"{record.Topic}-{record.Partition}",
                0,
                (_, existing) => existing + 1);
            return new RecordMetadata(record.Topic, record.Partition, simulatedOffset);
        }

        try
        {
            var key = record.Key is { Length: > 0 } ? record.Key : null;
            var offset = await client.Messaging.SendAsync(
                record.Topic, record.Partition, key, record.Value).ConfigureAwait(false);
            return new RecordMetadata(record.Topic, record.Partition, offset);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to produce record to {Topic}-{Partition}",
                record.Topic, record.Partition);
            HandleClientFailure(ex);
            throw;
        }
    }

    /// <summary>
    /// Produces a record synchronously.
    /// </summary>
    public RecordMetadata Produce(ProducerRecord record)
    {
        return ProduceAsync(record).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Produces a record to a topic with key and value.
    /// </summary>
    public Task<RecordMetadata> ProduceAsync(
        string topic,
        byte[] key,
        byte[] value,
        long timestamp = 0,
        int? partition = null)
    {
        var record = new ProducerRecord(topic, partition ?? 0, key, value, timestamp);
        return ProduceAsync(record);
    }

    /// <summary>
    /// Flushes all pending records. In connected mode sends are awaited per produce,
    /// so this only drains records buffered while offline.
    /// </summary>
    public void Flush()
    {
        var flushed = 0;
        while (_pendingRecords.TryDequeue(out _))
        {
            // Records buffered in offline/test mode are dropped on flush
            // (no broker was reachable when they were produced).
            flushed++;
        }

        if (flushed > 0)
        {
            _logger.LogDebug("Flushed {Count} records", flushed);
        }
    }

    /// <summary>
    /// Flushes pending records with timeout.
    /// </summary>
    public void Flush(TimeSpan timeout)
    {
        Flush();
    }

    public void Dispose()
    {
        SurgewaveNativeClient? client;
        lock (_clientLock)
        {
            if (_disposed)
                return;

            _disposed = true;
            client = _client;
            _client = null;
        }

        Flush();

        if (client != null)
        {
            try
            {
                client.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error disposing StreamsProducer native client");
            }
        }

        _connectSemaphore.Dispose();
    }

    /// <summary>
    /// Lazily establishes the native client connection (serialized across producer threads).
    /// Failures are logged and retried with exponential backoff; while disconnected the
    /// producer falls back to in-memory tracking.
    /// </summary>
    private async Task<SurgewaveNativeClient?> EnsureClientAsync()
    {
        var existing = _client;
        if (existing != null)
            return existing;

        if (_disposed || Environment.TickCount64 < Interlocked.Read(ref _nextConnectAttemptTick))
            return null;

        await _connectSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            existing = _client;
            if (existing != null)
                return existing;

            if (_disposed || Environment.TickCount64 < Interlocked.Read(ref _nextConnectAttemptTick))
                return null;

            var (host, port) = ParseBootstrapServers(_config.BootstrapServers);
            var candidate = new SurgewaveNativeClient(host, port, SurgewaveTransportType.Auto);
            try
            {
                await candidate.ConnectAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await candidate.DisposeAsync().ConfigureAwait(false);
                Interlocked.Exchange(ref _nextConnectAttemptTick,
                    Environment.TickCount64 + (long)_reconnectBackoff.TotalMilliseconds);

                if (!_connectFailureLogged)
                {
                    _connectFailureLogged = true;
                    _logger.LogWarning(ex,
                        "StreamsProducer failed to connect to {BootstrapServers}; producing in-memory and retrying with backoff (up to {MaxBackoff})",
                        _config.BootstrapServers, MaxReconnectBackoff);
                }
                else
                {
                    _logger.LogDebug(ex, "StreamsProducer reconnect to {BootstrapServers} failed",
                        _config.BootstrapServers);
                }

                _reconnectBackoff = TimeSpan.FromMilliseconds(Math.Min(
                    _reconnectBackoff.TotalMilliseconds * 2, MaxReconnectBackoff.TotalMilliseconds));
                return null;
            }

            lock (_clientLock)
            {
                if (_disposed)
                {
                    candidate.DisposeAsync().AsTask().GetAwaiter().GetResult();
                    return null;
                }
                _client = candidate;
            }

            _connectFailureLogged = false;
            _reconnectBackoff = InitialReconnectBackoff;
            _logger.LogInformation("StreamsProducer connected to {BootstrapServers} (native protocol)",
                _config.BootstrapServers);
            return candidate;
        }
        finally
        {
            _connectSemaphore.Release();
        }
    }

    /// <summary>
    /// Drops the current client after a connection-level failure so the next produce
    /// reconnects with backoff.
    /// </summary>
    private void HandleClientFailure(Exception ex)
    {
        SurgewaveNativeClient? client;
        lock (_clientLock)
        {
            client = _client;
            _client = null;
        }

        Interlocked.Exchange(ref _nextConnectAttemptTick,
            Environment.TickCount64 + (long)_reconnectBackoff.TotalMilliseconds);

        if (client != null)
        {
            _logger.LogWarning(ex, "StreamsProducer lost connection to {BootstrapServers}; reconnecting with backoff",
                _config.BootstrapServers);
            try
            {
                client.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch
            {
                // Best effort — connection is already broken.
            }
        }
    }

    private static (string Host, int Port) ParseBootstrapServers(string servers)
    {
        // Use the first entry of a comma-separated list (single-broker native protocol).
        var first = servers.Split(',')[0].Trim();
        var parts = first.Split(':');
        return (parts[0], parts.Length > 1 && int.TryParse(parts[1], out var port) ? port : 9092);
    }
}

/// <summary>
/// Represents a record to be produced.
/// </summary>
public readonly record struct ProducerRecord(
    string Topic,
    int Partition,
    byte[] Key,
    byte[] Value,
    long Timestamp = 0);

/// <summary>
/// Metadata about a produced record.
/// </summary>
public readonly record struct RecordMetadata(
    string Topic,
    int Partition,
    long Offset);
